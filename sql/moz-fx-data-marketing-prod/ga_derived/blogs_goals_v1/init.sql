CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.blogs_goals_v1`(
    date DATE,
    visitIdentifier STRING,
    browser STRING,
    downloads INT64,
    share INT64,
    newsletterSubscription INT64,
  )
PARTITION BY
  date
OPTIONS
  (description = "Key meterics per visit on the blogs webpage")
