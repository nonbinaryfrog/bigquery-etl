CREATE OR REPLACE TABLE
  login_flows_v1
AS
SELECT
  COALESCE(
    jsonPayload.user_properties.flow_id,
    JSON_EXTRACT_SCALAR(jsonPayload.fields.user_properties, "$.flow_id")
  ) AS flow_id,
  ARRAY_AGG(jsonPayload.fields.user_id IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS fxa_uid,
  MIN(`timestamp`) AS flow_started,
  LOGICAL_OR(jsonPayload.fields.event_type = "fxa_email_first - view") AS viewed_email_first_page,
FROM
  firefox_accounts_derived.fxa_content_events_v1
WHERE
  COALESCE(
    jsonPayload.event_properties.service,
    JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, "$.service")
  ) = "guardian-vpn"
GROUP BY
  flow_id
