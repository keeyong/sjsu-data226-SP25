CREATE TABLE dev.raw_data.vital (
    UserID int,
    VitalID	int PRIMARY KEY,
    Date date,
    Weight int
)
;

INSERT INTO dev.raw_data.vital VALUES
(100,	1, '2020-01-01', 75),
(100, 3, '2020-01-02', 78),
(101, 2, '2020-01-01', 90),
(101, 4, '2020-01-02', 95);

CREATE TABLE dev.raw_data.alert (
    AlertID int PRIMARY KEY,
    VitalID	int,
    AlertType varchar(32),
    Date date,
    UserID int
);
INSERT INTO dev.raw_data.alert VALUES
 (1,	4, 'WeightIncrease', '2020-01-01', 101),
 (2, NULL, 'MissingVital', '2020-01-04', 100),
 (3, NULL, 'MissingVital', '2020-01-04', 101);

-- INNER JOIN
SELECT *
FROM dev.raw_data.vital v
JOIN dev.raw_data.alert a ON v.vitalid = a.vitalid;

-- LEFT JOIN
SELECT *
FROM dev.raw_data.vital v
LEFT JOIN dev.raw_data.alert a ON v.vitalid = a.vitalid;

-- RIGHT JOIN
SELECT *
FROM dev.raw_data.vital v
RIGHT JOIN dev.raw_data.alert a ON v.vitalid = a.vitalid;

-- FULL JOIN
SELECT *
FROM dev.raw_data.vital v
FULL JOIN dev.raw_data.alert a ON v.vitalid = a.vitalid;

-- CROSS JOIN
SELECT *
FROM dev.raw_data.vital v
CROSS JOIN dev.raw_data.alert a;

-- SELF JOIN
SELECT *
FROM dev.raw_data.vital v1
LEFT JOIN dev.raw_data.vital v2 ON v1.vitalid = v2.vitalid;
