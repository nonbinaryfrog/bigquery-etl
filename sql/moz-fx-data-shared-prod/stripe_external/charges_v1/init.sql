CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod`.stripe_external.charges_v1
PARTITION BY
  DATE(created)
CLUSTER BY
  created
AS
WITH events AS (
  SELECT
    created AS event_timestamp,
    `data`.charge.*,
  FROM
    events_v1
  WHERE
    `data`.charge IS NOT NULL
  UNION ALL
  SELECT
    created AS event_timestamp,
    *,
  FROM
    initial_charges_v1
)
SELECT
  id,
  ARRAY_AGG(events ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].* EXCEPT (id),
FROM
  events
GROUP BY
  id
