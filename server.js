require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Octokit } = require('@octokit/rest');
const path = require('path');
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// GitHub Client
const octokit = new Octokit({
  auth: process.env.GITHUB_TOKEN,
});

const captureRawBody = (req, res, buf) => {
  req.rawBody = buf;
};

app.use(express.json({ verify: captureRawBody, limit: '2mb' }));
app.use(express.urlencoded({ extended: true, verify: captureRawBody, limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// In-Memory Database (or connect to MongoDB/PostgreSQL)
const teamMetrics = {};

const processedDeliveries = new Set();
const MAX_TRACKED_DELIVERIES = 500;

const STATE_DIR =
  process.env.STATE_DIR || path.join(os.homedir(), '.github-hackathon-tracker');
const STATE_FILE = process.env.STATE_FILE || path.join(STATE_DIR, 'state.json');

function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) {
      return;
    }

    const state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));

    if (state.teamMetrics && typeof state.teamMetrics === 'object') {
      Object.assign(teamMetrics, state.teamMetrics);
    }

    if (Array.isArray(state.processedDeliveries)) {
      for (const deliveryId of state.processedDeliveries.slice(-MAX_TRACKED_DELIVERIES)) {
        if (typeof deliveryId === 'string') {
          processedDeliveries.add(deliveryId);
        }
      }
    }

    console.log(
      `[State] Loaded ${Object.keys(teamMetrics).length} team(s) and ${processedDeliveries.size} delivery record(s)`
    );
  } catch (err) {
    console.error('[State] Failed to load persisted state:', err.message);
  }
}

function persistState() {
  try {
    fs.mkdirSync(path.dirname(STATE_FILE), { recursive: true });

    const state = {
      teamMetrics,
      processedDeliveries: Array.from(processedDeliveries),
      savedAt: new Date().toISOString(),
    };
    const serialized = JSON.stringify(state, null, 2);
    const tempFile = `${STATE_FILE}.tmp`;

    fs.writeFileSync(tempFile, serialized);

    try {
      fs.renameSync(tempFile, STATE_FILE);
    } catch (err) {
      fs.writeFileSync(STATE_FILE, serialized);
      fs.rmSync(tempFile, { force: true });
    }
  } catch (err) {
    console.error('[State] Failed to persist state:', err.message);
  }
}

loadState();

function calculateSummary(metrics) {
  const teams = Object.values(metrics);

  return {
    totalProjects: teams.length,
    totalCommits: teams.reduce((sum, team) => sum + (Number(team.totalCommits) || 0), 0),
    totalVideos: teams.filter((team) => team.hasVideo).length,
    totalPresentations: teams.filter((team) => team.hasPresentation).length,
  };
}

function buildMetricsPayload() {
  return {
    teams: teamMetrics,
    summary: calculateSummary(teamMetrics),
  };
}

function broadcastMetrics() {
  io.emit('metrics-updated', buildMetricsPayload());
}

function normalizePayload(req) {
  const body = req.body || {};
  if (typeof body.payload === 'string') {
    return JSON.parse(body.payload);
  }
  return body;
}

function verifySignature(req) {
  const secret = process.env.WEBHOOK_SECRET;
  if (!secret) {
    console.warn('[GitHub Webhook] WEBHOOK_SECRET is not set; skipping signature verification');
    return true;
  }

  const header = req.get('x-hub-signature-256');
  if (!header || !header.startsWith('sha256=')) {
    return false;
  }

  const expected = crypto
    .createHmac('sha256', secret)
    .update(req.rawBody || Buffer.alloc(0))
    .digest();
  const received = Buffer.from(header.slice('sha256='.length), 'hex');

  return expected.length === received.length && crypto.timingSafeEqual(expected, received);
}

function hasProcessedDelivery(deliveryId) {
  return Boolean(deliveryId && processedDeliveries.has(deliveryId));
}

function rememberDelivery(deliveryId) {
  if (!deliveryId) {
    return;
  }

  processedDeliveries.add(deliveryId);
  if (processedDeliveries.size > MAX_TRACKED_DELIVERIES) {
    processedDeliveries.delete(processedDeliveries.values().next().value);
  }
}

/**
 * Detect artifact files from the push payload. This is a fallback for private
 * repos the GITHUB_TOKEN cannot inspect; it only sets positive flags.
 */
function detectPayloadSignals(body) {
  const signals = {
    hasReadme: false,
    hasVideo: false,
    hasPresentation: false,
  };

  const changedFiles = new Set();
  const commits = Array.isArray(body?.commits) ? body.commits : [];

  for (const commit of commits) {
    for (const file of Array.isArray(commit?.added) ? commit.added : []) {
      changedFiles.add(file);
    }
    for (const file of Array.isArray(commit?.modified) ? commit.modified : []) {
      changedFiles.add(file);
    }
  }

  for (const file of changedFiles) {
    if (typeof file !== 'string' || file.includes('/')) {
      continue;
    }

    const name = file.toLowerCase();

    if (name.startsWith('readme')) {
      signals.hasReadme = true;
    }
    if (name.includes('demo') || name.endsWith('.mp4') || name.endsWith('.mov')) {
      signals.hasVideo = true;
    }
    if (name.includes('slides') || name.endsWith('.pdf') || name.endsWith('.pptx')) {
      signals.hasPresentation = true;
    }
  }

  return signals;
}

/**
 * Trust the API snapshot when available. Fall back to files changed in the
 * push only for fields the API could not read, so an API failure no longer
 * turns an existing YES into NO.
 */
function mergeInspection(metrics, inspection, body) {
  const signals = detectPayloadSignals(body);

  if (inspection.hasReadme !== null) {
    metrics.hasReadme = inspection.hasReadme;
  } else if (signals.hasReadme) {
    metrics.hasReadme = true;
  }

  if (inspection.hasVideo !== null) {
    metrics.hasVideo = inspection.hasVideo;
  } else if (signals.hasVideo) {
    metrics.hasVideo = true;
  }

  if (inspection.hasPresentation !== null) {
    metrics.hasPresentation = inspection.hasPresentation;
  } else if (signals.hasPresentation) {
    metrics.hasPresentation = true;
  }
}

/**
 * Check if specific files or metadata exist in the repository.
 * Returns null for any value Octokit could not determine, so callers
 * can preserve the previous state instead of reporting a false NO.
 */
async function inspectRepository(owner, repo) {
  const inspection = {
    hasReadme: null,
    hasVideo: null,
    hasPresentation: null,
  };

  let rootFiles = null;

  try {
    // 1. Check for README
    const readmeRes = await octokit.rest.repos.getReadme({ owner, repo });
    inspection.hasReadme = Boolean(readmeRes.data && readmeRes.data.size > 0);
  } catch (err) {
    if (err.status !== 404) {
      console.error(
        `[GitHub] README check failed for ${owner}/${repo}: ${err.status || 'unknown'} ${err.message}`
      );
    }
  }

  try {
    // 2. Scan repository root files for video or presentation links/files
    const contentsRes = await octokit.rest.repos.getContent({ owner, repo, path: '' });
    if (Array.isArray(contentsRes.data)) {
      rootFiles = contentsRes.data.map((file) => file.name.toLowerCase());

      inspection.hasVideo = rootFiles.some(
        (name) => name.includes('demo') || name.endsWith('.mp4') || name.endsWith('.mov')
      );
      inspection.hasPresentation = rootFiles.some(
        (name) => name.includes('slides') || name.endsWith('.pdf') || name.endsWith('.pptx')
      );
    }
  } catch (err) {
    console.error(
      `[GitHub] Contents check failed for ${owner}/${repo}: ${err.status || 'unknown'} ${err.message}`
    );
  }

  // If the repo is readable but getReadme returned 404, use the root listing
  // to distinguish "no README" from "repo not accessible".
  if (inspection.hasReadme === null && rootFiles) {
    inspection.hasReadme = rootFiles.some((name) => name.startsWith('readme'));
  }

  return inspection;
}

// GitHub Webhook Endpoint
app.post('/api/github-webhook', async (req, res) => {
  const event = req.headers['x-github-event'];
  const deliveryId = req.get('x-github-delivery');

  if (!verifySignature(req)) {
    console.warn(
      `[GitHub Webhook] Rejected ${event || 'unknown'} delivery ${deliveryId || 'unknown'}: invalid signature`
    );
    return res.status(401).json({ error: 'Invalid signature' });
  }

  if (event === 'ping') {
    console.log('[GitHub Webhook] Ping received');
    return res.status(200).json({ status: 'success' });
  }

  if (event !== 'push') {
    return res.status(200).json({ status: 'ignored', event: event || 'unknown' });
  }

  let body;
  try {
    body = normalizePayload(req);
  } catch (err) {
    console.error('[GitHub Webhook] Failed to parse payload:', err.message);
    return res.status(400).json({ error: 'Invalid payload' });
  }

  const repository = body.repository;
  const ownerName = repository?.owner?.login;
  const repoName = repository?.name;
  const repoFullName = repository?.full_name;

  if (!ownerName || !repoName || !repoFullName) {
    console.error('[GitHub Webhook] Push payload is missing repository data');
    return res.status(400).json({ error: 'Invalid push payload' });
  }

  const commitsCount = Array.isArray(body.commits) ? body.commits.length : 0;
  const isDuplicate = hasProcessedDelivery(deliveryId);

  try {
    console.log(
      `[GitHub Webhook] Push received from ${repoFullName} (${commitsCount} commit(s))`
    );

    // Inspect files in repository
    const inspection = await inspectRepository(ownerName, repoName);

    // Update in-memory state
    if (!teamMetrics[repoFullName]) {
      teamMetrics[repoFullName] = {
        teamName: repoFullName,
        totalCommits: 0,
        hasReadme: false,
        hasVideo: false,
        hasPresentation: false,
        lastUpdated: new Date().toISOString(),
      };
    }

    const metrics = teamMetrics[repoFullName];
    mergeInspection(metrics, inspection, body);

    // A retried delivery can repair stale flags (for example after token
    // access changes) without double-counting commits.
    if (isDuplicate) {
      console.log(
        `[GitHub Webhook] Duplicate delivery ${deliveryId}; refreshed artifact flags only`
      );
      persistState();
      broadcastMetrics();
      return res.status(200).json({ status: 'duplicate' });
    }

    metrics.totalCommits += commitsCount;
    metrics.lastUpdated = new Date().toISOString();

    rememberDelivery(deliveryId);

    persistState();

    // Broadcast live event to frontend dashboard
    broadcastMetrics();

    return res.status(200).json({ status: 'success' });
  } catch (err) {
    console.error(`[GitHub Webhook] Failed to process push from ${repoFullName}:`, err);
    return res.status(500).json({ error: 'Failed to process webhook' });
  }
});

// REST API endpoint to load initial metrics on page refresh
app.get('/api/metrics', (req, res) => {
  res.json(teamMetrics);
});

// Real-Time Socket Connection
io.on('connection', (socket) => {
  console.log('Dashboard client connected:', socket.id);
  socket.emit('metrics-updated', buildMetricsPayload());
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`GitHub Tracker Server running at http://localhost:${PORT}`);
});