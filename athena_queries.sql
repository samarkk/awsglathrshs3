SELECT ARRAY [1,2,3,4] AS items

-----------------------------------------------------------------------------

SELECT ARRAY[ ARRAY[1,2], ARRAY[3,4] ] AS items

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT 1 AS x, 2 AS y, 3 AS z
)
SELECT ARRAY [x,y,z] AS items FROM dataset

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT
ARRAY ['hello', 'amazon', 'athena'] AS words,
ARRAY ['hi', 'alexa'] AS alexa
)
SELECT ARRAY[words, alexa] AS welcome_msg
FROM dataset

-----------------------------------------------------------------------------

SELECT ARRAY[
MAP(ARRAY['first', 'last', 'age'],ARRAY['Bob', 'Smith', '40']),
MAP(ARRAY['first', 'last', 'age'],ARRAY['Jane', 'Doe', '30']),
MAP(ARRAY['first', 'last', 'age'],ARRAY['Billy', 'Smith', '8'])
] AS people

-----------------------------------------------------------------------------

SELECT ARRAY [4,5] || ARRAY[ ARRAY[1,2], ARRAY[3,4] ] AS items

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT
ARRAY ['hello', 'amazon', 'athena'] AS words,
ARRAY ['hi', 'alexa'] AS alexa
)
SELECT concat(words, alexa) AS welcome_msg
FROM dataset

-----------------------------------------------------------------------------

SELECT
ARRAY [CAST(4 AS VARCHAR), CAST(5 AS VARCHAR)]
AS items

-----------------------------------------------------------------------------

SELECT
ARRAY[CAST(MAP(ARRAY['a1', 'a2', 'a3'], ARRAY[1, 2, 3]) AS JSON)] ||
ARRAY[CAST(MAP(ARRAY['b1', 'b2', 'b3'], ARRAY[4, 5, 6]) AS JSON)]
AS items

-----------------------------------------------------------------------------

SELECT cardinality(ARRAY[1,2,3,4]) AS item_count

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT
ARRAY[CAST(MAP(ARRAY['a1', 'a2', 'a3'], ARRAY[1, 2, 3]) AS JSON)] ||
ARRAY[CAST(MAP(ARRAY['b1', 'b2', 'b3'], ARRAY[4, 5, 6]) AS JSON)]
AS items )
SELECT items[1] AS item FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT ARRAY ['hello', 'amazon', 'athena'] AS words
)
SELECT
element_at(words, 1) AS first_word,
element_at(words, -2) AS middle_word,
element_at(words, cardinality(words)) AS last_word
FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT
'engineering' as department,
ARRAY['Sharon', 'John', 'Bob', 'Sally'] as users
)
SELECT department, names FROM dataset
CROSS JOIN UNNEST(users) as t(names)

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT
'engineering' as department,
ARRAY[
MAP(ARRAY['first', 'last', 'age'],ARRAY['Bob', 'Smith', '40']),
MAP(ARRAY['first', 'last', 'age'],ARRAY['Jane', 'Doe', '30']),
MAP(ARRAY['first', 'last', 'age'],ARRAY['Billy', 'Smith', '8'])
] AS people
)
SELECT names['first'] AS
first_name,
names['last'] AS last_name,
department FROM dataset
CROSS JOIN UNNEST(people) AS t(names)

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT ARRAY[
CAST(ROW('Sally', 'engineering', ARRAY[1,2,3,4]) AS ROW(name VARCHAR, department
VARCHAR, scores ARRAY(INTEGER))),
CAST(ROW('John', 'finance', ARRAY[7,8,9]) AS ROW(name VARCHAR, department VARCHAR,
scores ARRAY(INTEGER))),
CAST(ROW('Amy', 'devops', ARRAY[12,13,14,15]) AS ROW(name VARCHAR, department VARCHAR,
scores ARRAY(INTEGER)))
] AS users
),
users AS (
SELECT person, score
FROM
dataset,
UNNEST(dataset.users) AS t(person),
UNNEST(person.scores) AS t(score)
)
SELECT person.name, person.department, SUM(score) AS total_score FROM users
GROUP BY (person.name, person.department)
ORDER BY (total_score) DESC
LIMIT 1

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT ARRAY[
CAST(ROW('Sally', 'engineering', ARRAY[1,2,3,4]) AS ROW(name VARCHAR, department
VARCHAR, scores ARRAY(INTEGER))),
CAST(ROW('John', 'finance', ARRAY[7,8,9]) AS ROW(name VARCHAR, department VARCHAR,
scores ARRAY(INTEGER))),
CAST(ROW('Amy', 'devops', ARRAY[12,13,14,15]) AS ROW(name VARCHAR, department VARCHAR,
scores ARRAY(INTEGER)))
] AS users
),
users AS (
SELECT person, score
FROM
dataset,
UNNEST(dataset.users) AS t(person),
UNNEST(person.scores) AS t(score)
)
SELECT person.name, score FROM users
ORDER BY (score) DESC
LIMIT 1

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT ARRAY[1,2,3,4,5] AS items
)
SELECT array_agg(i) AS array_items
FROM dataset
CROSS JOIN UNNEST(items) AS t(i)

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT ARRAY [1,2,2,3,3,4,5] AS items
)
SELECT array_agg(distinct i) AS array_items
FROM dataset
CROSS JOIN UNNEST(items) AS t(i)

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT ARRAY[1,2,3,4,5] AS items
)
SELECT array_agg(i) AS array_items
FROM dataset
CROSS JOIN UNNEST(items) AS t(i)
WHERE i > 3

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT ARRAY
[
ARRAY[1,2,3,4],
ARRAY[5,6,7,8],
ARRAY[9,0]
] AS items
)
SELECT i AS array_items FROM dataset
CROSS JOIN UNNEST(items) AS t(i)
WHERE contains(i, 2)

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT ARRAY[3,1,2,5,2,3,6,3,4,5] AS items
)
SELECT array_sort(array_agg(distinct i)) AS array_items
FROM dataset
CROSS JOIN UNNEST(items) AS t(i)

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT ARRAY
[
ARRAY[1,2,3,4],
ARRAY[5,6,7,8],
ARRAY[9,0]
] AS items
),
item AS (
SELECT i AS array_items
FROM dataset, UNNEST(items) AS t(i)
)
SELECT array_items, sum(val) AS total
FROM item, UNNEST(array_items) AS t(val)

-----------------------------------------------------------------------------

WITH
dataset AS (
SELECT ARRAY ['hello', 'amazon', 'athena'] AS words
)
SELECT array_join(words, ' ') AS welcome_msg
FROM dataset
GROUP BY array_items

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT
ROW('Bob', 38) AS users
)
SELECT * FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT
CAST(
ROW('Bob', 38) AS ROW(name VARCHAR, age INTEGER)
) AS users
)
SELECT * FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT ARRAY[
CAST(ROW('Bob', 38) AS ROW(name VARCHAR, age INTEGER)),
CAST(ROW('Alice', 35) AS ROW(name VARCHAR, age INTEGER)),
CAST(ROW('Jane', 27) AS ROW(name VARCHAR, age INTEGER))
] AS users
)
SELECT * FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT
CAST(
ROW('aws.amazon.com', ROW(true)) AS ROW(hostname VARCHAR, flaggedActivity ROW(isNew
BOOLEAN))
) AS sites
)
SELECT * FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT
CAST(
ROW('aws.amazon.com', ROW(true)) AS ROW(hostname VARCHAR, flaggedActivity ROW(isNew
BOOLEAN))
) AS sites
)
SELECT sites.hostname, sites.flaggedactivity.isnew
FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT ARRAY[
CAST(
ROW('aws.amazon.com', ROW(true)) AS ROW(hostname VARCHAR, flaggedActivity ROW(isNew
BOOLEAN))
),
CAST(
ROW('news.cnn.com', ROW(false)) AS ROW(hostname VARCHAR, flaggedActivity ROW(isNew
BOOLEAN))
),
CAST(
ROW('netflix.com', ROW(false)) AS ROW(hostname VARCHAR, flaggedActivity ROW(isNew
BOOLEAN))
)
] as items
)
SELECT sites.hostname, sites.flaggedActivity.isNew
FROM dataset, UNNEST(items) t(sites)
WHERE sites.flaggedActivity.isNew = true

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT ARRAY[
CAST(
ROW('aws.amazon.com', ROW(ARRAY[
MAP(ARRAY['term', 'count'], ARRAY['bigdata', '10']),
MAP(ARRAY['term', 'count'], ARRAY['serverless', '50']),
MAP(ARRAY['term', 'count'], ARRAY['analytics', '82']),
MAP(ARRAY['term', 'count'], ARRAY['iot', '74'])
])
) AS ROW(hostname VARCHAR, flaggedActivity ROW(flags ARRAY(MAP(VARCHAR, VARCHAR)) ))
),
CAST(
ROW('news.cnn.com', ROW(ARRAY[
MAP(ARRAY['term', 'count'], ARRAY['politics', '241']),
MAP(ARRAY['term', 'count'], ARRAY['technology', '211']),
MAP(ARRAY['term', 'count'], ARRAY['serverless', '25']),
MAP(ARRAY['term', 'count'], ARRAY['iot', '170'])
])
) AS ROW(hostname VARCHAR, flaggedActivity ROW(flags ARRAY(MAP(VARCHAR, VARCHAR)) ))
),
CAST(
ROW('netflix.com', ROW(ARRAY[
MAP(ARRAY['term', 'count'], ARRAY['cartoons', '1020']),
MAP(ARRAY['term', 'count'], ARRAY['house of cards', '112042']),
MAP(ARRAY['term', 'count'], ARRAY['orange is the new black', '342']),
MAP(ARRAY['term', 'count'], ARRAY['iot', '4'])
])
) AS ROW(hostname VARCHAR, flaggedActivity ROW(flags ARRAY(MAP(VARCHAR, VARCHAR)) ))
)
] AS items
),
sites AS (
SELECT sites.hostname, sites.flaggedactivity
FROM dataset, UNNEST(items) t(sites)
)
SELECT hostname
FROM sites, UNNEST(sites.flaggedActivity.flags) t(flags)
WHERE regexp_like(flags['term'], 'politics|bigdata')
GROUP BY (hostname)

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT ARRAY[
CAST(
ROW('aws.amazon.com', ROW(ARRAY[
MAP(ARRAY['term', 'count'], ARRAY['bigdata', '10']),
MAP(ARRAY['term', 'count'], ARRAY['serverless', '50']),
MAP(ARRAY['term', 'count'], ARRAY['analytics', '82']),
MAP(ARRAY['term', 'count'], ARRAY['iot', '74'])
])
) AS ROW(hostname VARCHAR, flaggedActivity ROW(flags ARRAY(MAP(VARCHAR, VARCHAR)) ))
),
CAST(
ROW('news.cnn.com', ROW(ARRAY[
MAP(ARRAY['term', 'count'], ARRAY['politics', '241']),
MAP(ARRAY['term', 'count'], ARRAY['technology', '211']),
MAP(ARRAY['term', 'count'], ARRAY['serverless', '25']),
MAP(ARRAY['term', 'count'], ARRAY['iot', '170'])
])
) AS ROW(hostname VARCHAR, flaggedActivity ROW(flags ARRAY(MAP(VARCHAR, VARCHAR)) ))
),
CAST(
ROW('netflix.com', ROW(ARRAY[
MAP(ARRAY['term', 'count'], ARRAY['cartoons', '1020']),
MAP(ARRAY['term', 'count'], ARRAY['house of cards', '112042']),
MAP(ARRAY['term', 'count'], ARRAY['orange is the new black', '342']),
MAP(ARRAY['term', 'count'], ARRAY['iot', '4'])
])
) AS ROW(hostname VARCHAR, flaggedActivity ROW(flags ARRAY(MAP(VARCHAR, VARCHAR)) ))
)
] AS items
),
sites AS (
SELECT sites.hostname, sites.flaggedactivity
FROM dataset, UNNEST(items) t(sites)
)
SELECT hostname, array_agg(flags['term']) AS terms, SUM(CAST(flags['count'] AS INTEGER)) AS
total
FROM sites, UNNEST(sites.flaggedActivity.flags) t(flags)
WHERE regexp_like(flags['term'], 'politics|bigdata')
GROUP BY (hostname)
ORDER BY total DESC

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT MAP(
ARRAY['first', 'last', 'age'],
ARRAY['Bob', 'Smith', '35']
) AS user
)
SELECT user FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT MAP(
ARRAY['first', 'last', 'age'],
ARRAY['Bob', 'Smith', '35']
) AS user
)
SELECT user['first'] AS first_name FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT
CAST('HELLO ATHENA' AS JSON) AS hello_msg,
CAST(12345 AS JSON) AS some_int,
CAST(MAP(ARRAY['a', 'b'], ARRAY[1,2]) AS JSON) AS some_map
)
SELECT * FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT
CAST(JSON '"HELLO ATHENA"' AS VARCHAR) AS hello_msg,
CAST(JSON '12345' AS INTEGER) AS some_int,
CAST(JSON '{"a":1,"b":2}' AS MAP(VARCHAR, INTEGER)) AS some_map
)
SELECT * FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT '{"name": "Susan Smith",
"org": "engineering",
"projects": [{"name":"project1", "completed":false},
{"name":"project2", "completed":true}]}'
AS blob
)
SELECT
json_extract(blob, '$.name') AS name,
json_extract(blob, '$.projects') AS projects

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT '{"name": "Susan Smith",
"org": "engineering",
"projects": [{"name":"project1", "completed":false},{"name":"project2",
"completed":true}]}'
AS blob
)
SELECT
json_extract_scalar(blob, '$.name') AS name,
json_extract_scalar(blob, '$.projects') AS projects
FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT '{"name": "Bob Smith",
"org": "engineering",
"projects": [{"name":"project1", "completed":false},{"name":"project2",
"completed":true}]}'
AS blob
)
SELECT json_array_get(json_extract(blob, '$.projects'), 0) AS item
FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT '{"name": "Bob Smith",
"org": "engineering",
"projects": [{"name":"project1", "completed":false},{"name":"project2",
"completed":true}]}'
AS blob
)
SELECT json_extract_scalar(blob, '$.projects[0].name') AS project_name
FROM dataset

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT * FROM (VALUES
(JSON '{"name": "Bob Smith", "org": "legal", "projects": ["project1"]}'),
(JSON '{"name": "Susan Smith", "org": "engineering", "projects": ["project1",
"project2", "project3"]}'),
(JSON '{"name": "Jane Smith", "org": "finance", "projects": ["project1", "project2"]}')
) AS t (users)
)
SELECT json_extract_scalar(users, '$.name') AS user
FROM dataset
WHERE json_array_contains(json_extract(users, '$.projects'), 'project2')

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT * FROM (VALUES
(JSON '{"name": "Bob Smith",
"org": "legal",
"projects": [{"name":"project1", "completed":false}]}'),
(JSON '{"name": "Susan Smith",
"org": "engineering",
"projects": [{"name":"project2", "completed":true},
{"name":"project3", "completed":true}]}'),
(JSON '{"name": "Jane Smith",
"org": "finance",
"projects": [{"name":"project2", "completed":true}]}')
) AS t (users)
),
employees AS (
SELECT users, CAST(json_extract(users, '$.projects') AS
ARRAY(MAP(VARCHAR, JSON))) AS projects_array
FROM dataset
),
names AS (
SELECT json_extract_scalar(users, '$.name') AS name, projects
FROM employees, UNNEST (projects_array) AS t(projects)
)
SELECT name, count(projects) AS completed_projects FROM names
WHERE cast(element_at(projects, 'completed') AS BOOLEAN) = true
GROUP BY name

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT * FROM (VALUES
(JSON '{"name":
"Bob Smith",
"org":
"legal",
"projects": [{"name":"project1", "completed":false}]}'),
(JSON '{"name": "Susan Smith",
"org": "engineering",
"projects": [{"name":"project2", "completed":true},
{"name":"project3", "completed":true}]}'),
(JSON '{"name": "Jane Smith",
"org": "finance",
"projects": [{"name":"project2", "completed":true}]}')
) AS t (users)
)
SELECT
json_extract_scalar(users, '$.name') as name,
json_array_length(json_extract(users, '$.projects')) as count
FROM dataset
ORDER BY count DESC

-----------------------------------------------------------------------------

WITH dataset AS (
SELECT * FROM (VALUES
(JSON '{"name": "Bob Smith", "org": "legal", "projects": [{"name":"project1",
"completed":false}]}'),
(JSON '{"name": "Susan Smith", "org": "engineering", "projects": [{"name":"project2",
"completed":true},{"name":"project3", "completed":true}]}'),
(JSON '{"name": "Jane Smith", "org": "finance", "projects": [{"name":"project2",
"completed":true}]}')
) AS t (users)
)
SELECT
json_extract_scalar(users, '$.name') as name,
json_size(users, '$.projects') as count
FROM dataset
ORDER BY count DESC

-----------------------------------------------------------------------------
