require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Octokit } = require('@octokit/rest');
const path = require('path');
const crypto = require('crypto');

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
 * Check if specific files or metadata exist in the repository
 */
async function inspectRepository(owner, repo) {
  let hasReadme = false;
  let hasVideo = false;
  let hasPresentation = false;

  try {
    // 1. Check for README
    const readmeRes = await octokit.rest.repos.getReadme({ owner, repo });
    if (readmeRes.data && readmeRes.data.size > 0) {
      hasReadme = true;
    }
  } catch (err) {
    hasReadme = false;
  }

  try {
    // 2. Scan repository root files for video or presentation links/files
    const contentsRes = await octokit.rest.repos.getContent({ owner, repo, path: '' });
    if (Array.isArray(contentsRes.data)) {
      const fileNames = contentsRes.data.map((f) => f.name.toLowerCase());

      hasVideo = fileNames.some(
        (name) => name.includes('demo') || name.endsWith('.mp4') || name.endsWith('.mov')
      );
      hasPresentation = fileNames.some(
        (name) => name.includes('slides') || name.endsWith('.pdf') || name.endsWith('.pptx')
      );
    }
  } catch (err) {
    console.error(`Error reading repo contents for ${repo}:`, err.message);
  }

  return { hasReadme, hasVideo, hasPresentation };
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

  if (hasProcessedDelivery(deliveryId)) {
    console.log(`[GitHub Webhook] Duplicate delivery ignored: ${deliveryId}`);
    return res.status(200).json({ status: 'duplicate' });
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

    teamMetrics[repoFullName].totalCommits += commitsCount;
    teamMetrics[repoFullName].hasReadme = inspection.hasReadme;
    teamMetrics[repoFullName].hasVideo = inspection.hasVideo;
    teamMetrics[repoFullName].hasPresentation = inspection.hasPresentation;
    teamMetrics[repoFullName].lastUpdated = new Date().toISOString();

    rememberDelivery(deliveryId);

    // Broadcast live event to frontend dashboard
    io.emit('metrics-updated', teamMetrics);

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
  socket.emit('metrics-updated', teamMetrics);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`GitHub Tracker Server running at http://localhost:${PORT}`);
});