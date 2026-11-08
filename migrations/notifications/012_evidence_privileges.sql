-- Repair inherited default privileges without rewriting applied migrations.
-- Attempts/outcomes/blocked-admission receipts are append-only evidence.
REVOKE UPDATE, DELETE, TRUNCATE ON
    app.delivery_attempts,
    app.delivery_successes,
    app.delivery_uncertainties,
    app.delivery_blocks,
    app.delivery_block_parks,
    app.delivery_retryables
FROM notifications_runtime;

-- Reports retain UPDATE for acknowledgement, but cannot be removed by runtime.
REVOKE DELETE, TRUNCATE ON
    app.delivery_success_reports,
    app.delivery_uncertain_reports,
    app.delivery_blocked_reports,
    app.delivery_retryable_reports
FROM notifications_runtime;
