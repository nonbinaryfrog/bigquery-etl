WITH with_hits AS (
SELECT
  PARSE_DATE('%Y%m%d', date) AS date,
  CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string)) AS visitIdentifier,
  device.deviceCategory,
  device.operatingSystem,
  device.browser,
  device.language,
  geoNetwork.country AS country,
  trafficSource.source AS source,
  trafficSource.medium AS medium,
  trafficSource.campaign AS campaign,
  trafficSource.adcontent AS content,
  hits.page.pagePath AS landingPage,
  CASE
  WHEN
    hits.isEntrance IS NOT NULL
  THEN
    1
  ELSE
    0
  END
  AS entrance,
  SPLIT(hits.page.pagePathLevel1, '/')[SAFE_OFFSET(1)] AS blog,
  SPLIT(hits.page.pagePathLevel2, '/')[SAFE_OFFSET(1)] AS pagePathLevel2
FROM
  `ga-mozilla-org-prod-001.66602784.ga_sessions_*`
CROSS JOIN
  UNNEST(hits) AS hits
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
),
sessions_table AS (
SELECT
  date,
  visitIdentifier,
  deviceCategory,
  operatingSystem,
  browser,
  language,
  country,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog,
  SUM(sessions) AS sessions,
FROM
  (
    SELECT
  * EXCEPT(blog, pagePathLevel2),
  CASE
  WHEN
    blog LIKE "press%"
  THEN
    "press"
  WHEN
    blog = 'firefox'
  THEN
    'The Firefox Frontier'
  WHEN
    blog = 'netPolicy'
  THEN
    'Open Policy & Advocacy'
  WHEN
    LOWER(blog) = 'internetcitizen'
  THEN
    'Internet Citizen'
  WHEN
    blog = 'futurereleases'
  THEN
    'Future Releases'
  WHEN
    blog = 'careers'
  THEN
    'Careers'
  WHEN
    blog = 'opendesign'
  THEN
    'Open Design'
  WHEN
    blog = ""
  THEN
    "Blog Home Page"
  WHEN
    LOWER(blog) IN (
      'blog',
      'addons',
      'security',
      'opendesign',
      'nnethercote',
      'thunderbird',
      'community',
      'l10n',
      'theden',
      'webrtc',
      'berlin',
      'webdev',
      'services',
      'tanvi',
      'laguaridadefirefox',
      'ux',
      'fxtesteng',
      'foundation-archive',
      'nfroyd',
      'sumo',
      'javascript',
      'page',
      'data'
    )
  THEN
    LOWER(blog)
  ELSE
    'other'
  END
  AS blog,
  CASE
  WHEN
    blog = "firefox"
    AND pagePathLevel2 IN ('ru', 'pt-br', 'pl', 'it', 'id', 'fr', 'es', 'de')
  THEN
    pagePathLevel2
  WHEN
    blog = "firefox"
  THEN
    "Main"
  WHEN
    blog LIKE "press-%"
    AND blog IN (
      'press-de',
      'press-fr',
      'press-es',
      'press-uk',
      'press-pl',
      'press-it',
      'press-br',
      'press-nl'
    )
  THEN
    blog
  WHEN
    blog LIKE "press%"
  THEN
    "Main"
  WHEN
    blog = 'internetcitizen'
    AND pagePathLevel2 IN ('de', 'fr')
  THEN
    pagePathLevel2
  ELSE
    "Main"
  END
  AS subblog,
  ROW_NUMBER() OVER (
    PARTITION BY
      visitIdentifier
    ORDER BY
      visitIdentifier,
      entrance
  ) AS entryPage,
  COUNT(DISTINCT visitIdentifier) AS sessions
FROM
  with_hits
GROUP BY
  date,
  visitIdentifier,
  deviceCategory,
  operatingSystem,
  browser,
  language,
  country,
  source,
  medium,
  campaign,
  content,
  landingPage,
  blog,
  subblog,
  entrance
  )
WHERE
  entryPage = 1
GROUP BY
  date,
  visitIdentifier,
  deviceCategory,
  operatingSystem,
  browser,
  language,
  country,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog
),
-- Some pages have a landing page = (not set). This makes it difficult to match the totals for the property to the landing page totals
-- By joining the landing page totals to the summary total, we can isolate the (not set) sessions
landing_page_table AS (
  SELECT
    date,
    visitIdentifier,
    landingPage,
    cleanedLandingPage,
    SUM(sessions) AS page_sessions
  FROM
    (
      SELECT
        PARSE_DATE("%Y%m%d", date) AS date,
        CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string)) AS visitIdentifier,
        hits.page.pagePath AS landingPage,
        SPLIT(hits.page.pagePath, '?')[OFFSET(0)] AS cleanedLandingPage,
        CASE
        WHEN
          hits.isEntrance IS NOT NULL
        THEN
          1
        ELSE
          0
        END
        AS sessions
      FROM
        `ga-mozilla-org-prod-001.66602784.ga_sessions_*`,
        UNNEST(hits) AS hits
      WHERE
        _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    )
  WHERE
    sessions != 0
  GROUP BY
    1,
    2,
    3,
    4
),
goals_table AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    visitIdentifier,
    SUM(IF(downloads > 0, 1, 0)) AS downloads,
    SUM(IF(share > 0, 1, 0)) AS socialShare,
    SUM(IF(newsletterSubscription > 0, 1, 0)) AS newsletterSubscription
  FROM
    (
      SELECT
        date AS date,
        CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string)) AS visitIdentifier,
        device.browser,
        SUM(IF(hits.eventInfo.eventAction = "Firefox Download", 1, 0)) AS downloads,
        SUM(IF(hits.eventInfo.eventAction = "share", 1, 0)) AS share,
        SUM(
          IF(hits.eventInfo.eventAction = "newsletter subscription", 1, 0)
        ) AS newsletterSubscription
      FROM
        `ga-mozilla-org-prod-001.66602784.ga_sessions_*`,
        UNNEST(hits) AS hits
      WHERE
        _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
      GROUP BY
        date,
        visitIdentifier,
        browser
    )
  GROUP BY
    date,
    visitIdentifier
)
SELECT
  date,
  deviceCategory AS device_category,
  operatingSystem AS operating_system,
  browser,
  language,
  country,
  standardizedCountryList.standardizedCountry AS standardized_country_name,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog,
  landingPage AS landing_page,
  cleanedLandingPage AS cleaned_landing_page,
  SUM(sessions) AS sessions,
  SUM(downloads) AS downloads,
  SUM(socialShare) AS socialShare,
  SUM(newsletterSubscription) AS newsletterSubscription
FROM
  sessions_table
LEFT JOIN
  goals_table
USING
  (date, visitIdentifier)
LEFT JOIN
  landing_page_table
USING
  (date, visitIdentifier)
LEFT JOIN
  `moz-fx-data-marketing-prod.ga_derived.standardized_country_list` AS standardizedCountryList
ON
  sessions_table.country = standardizedCountryList.rawCountry
GROUP BY
  date,
  deviceCategory,
  operatingSystem,
  browser,
  language,
  country,
  standardized_country_name,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog,
  landingPage,
  cleanedLandingPage
