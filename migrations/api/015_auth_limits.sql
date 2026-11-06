-- API-owned shared authentication quota. One reusable row per canonical IP.
CREATE TABLE app.auth_limits (
    ip inet PRIMARY KEY CHECK (masklen(ip) = CASE family(ip) WHEN 4 THEN 32 ELSE 128 END),
    window_start timestamptz NOT NULL,
    used integer NOT NULL CHECK (used BETWEEN 0 AND 20)
);
