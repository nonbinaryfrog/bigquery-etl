WITH login_started AS (
  SELECT
    jsonPayload.fields.flow_id,
    MIN(`timestamp`) AS flow_started,
  FROM
    firefox_accounts_derived.fxa_content_events_v1
  WHERE
    jsonPayload.fields.service = "mozilla-vpn"
    AND DATE(`timestamp`) = @date
  GROUP BY
    flow_id
),
login_success AS (
  SELECT
    flow_id,
    ANY_VALUE(jsonPayload.fields.user_id) AS fxa_uid,
  FROM
    firefox_accounts_derived.fxa_auth_events_v1
  WHERE
    jsonPayload.fields.service = "mozilla-vpn"
    AND jsonPayload.fields.user_id IS NOT NULL
    AND DATE(`timestamp`) = @date
  GROUP BY
    flow_id
),
_current AS (
  SELECT
    flow_id,
    flow_started,
    fxa_uid
  FROM
    login_started
  JOIN
    login_success
  USING
    (flow_id)
)
SELECT
  flow_id,
  IF(
    _previous.flow_started IS NULL
    OR _previous.flow_started > _current.flow_started,
    _current,
    _previous
  ).flow_started COALESE(_previous.fxa_uid, _current.fxa_uid) AS fxa_uid,
FROM
  login_flows_v1 AS _previous
FULL JOIN
  _current
USING
  (flow_id)
