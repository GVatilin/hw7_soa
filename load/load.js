import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 10,
  duration: "30s",
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<3000"],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://producer:8000";

function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (char) => {
    const value = Math.floor(Math.random() * 16);
    const replacement = char === "x" ? value : (value & 0x3) | 0x8;
    return replacement.toString(16);
  });
}

export default function () {
  const now = new Date();
  const id = `${__VU}-${__ITER}-${now.getTime()}`;
  const payload = JSON.stringify({
    event_id: uuidv4(),
    user_id: `load-user-${__VU}`,
    movie_id: `movie-${(__ITER % 20) + 1}`,
    event_type: __ITER % 5 === 0 ? "VIEW_FINISHED" : "VIEW_STARTED",
    timestamp: now.toISOString(),
    device_type: ["MOBILE", "DESKTOP", "TV", "TABLET"][__ITER % 4],
    session_id: `load-session-${id}`,
    progress_seconds: __ITER % 5 === 0 ? 1200 : 0,
  });

  const response = http.post(`${BASE_URL}/events`, payload, {
    headers: {"Content-Type": "application/json"},
    tags: {name: "publish_event"},
  });

  check(response, {
    "event accepted": (r) => r.status === 200,
    "response has published status": (r) => r.json("status") === "published",
  });

  sleep(0.2);
}
