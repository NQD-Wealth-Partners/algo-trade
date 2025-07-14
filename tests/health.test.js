import { test } from 'node:test';
import assert from 'node:assert';
import supertest from 'supertest';
import app from '../server.js';

const request = supertest(app);

test('Health Endpoints', async (t) => {
  await t.test('should get API health status', async () => {
    const res = await request.get('/api/v1/health');
    
    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.body.status, 'success');
    assert.strictEqual(res.body.data.status, 'OK');
    assert.ok(res.body.data.hasOwnProperty('timestamp'));
    assert.ok(res.body.data.hasOwnProperty('uptime'));
  });
  
  await t.test('should get Redis health status', async () => {
    const res = await request.get('/api/v1/health/redis');
    
    assert.strictEqual(res.statusCode, 200);
    assert.strictEqual(res.body.status, 'success');
    assert.strictEqual(res.body.data.status, 'OK');
    assert.ok(res.body.data.redis.hasOwnProperty('connected'));
    assert.strictEqual(res.body.data.redis.connected, true);
  });
});