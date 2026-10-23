require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Octokit } = require('@octokit/rest');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// GitHub Client
const octokit = new Octokit({
  auth: process.env.GITHUB_TOKEN,
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// In-Memory Database (or connect to MongoDB/PostgreSQL)
const teamMetrics = {};

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
  const body = req.body;

  if (event === 'push') {
    const repoName = body.repository.name;
    const ownerName = body.repository.owner.login;
    const commitsCount = body.commits ? body.commits.length : 0;

    console.log(`[GitHub Webhook] Push received from ${ownerName}/${repoName}`);

    // Inspect files in repository
    const inspection = await inspectRepository(ownerName, repoName);

    // Update in-memory state
    if (!teamMetrics[repoName]) {
      teamMetrics[repoName] = {
        teamName: repoName,
        totalCommits: 0,
        hasReadme: false,
        hasVideo: false,
        hasPresentation: false,
        lastUpdated: new Date().toISOString(),
      };
    }

    teamMetrics[repoName].totalCommits += commitsCount;
    teamMetrics[repoName].hasReadme = inspection.hasReadme;
    teamMetrics[repoName].hasVideo = inspection.hasVideo;
    teamMetrics[repoName].hasPresentation = inspection.hasPresentation;
    teamMetrics[repoName].lastUpdated = new Date().toISOString();

    // Broadcast live event to frontend dashboard
    io.emit('metrics-updated', teamMetrics);
  }

  res.status(200).json({ status: 'success' });
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