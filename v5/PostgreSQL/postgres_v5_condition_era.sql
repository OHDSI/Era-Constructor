--------------------------------------------------------------------------------------------------------------
---Adapted to PostgreSQL condition_era from Pure SQL drug_era written by Chris_Knoll: https://gist.github.com/chrisknoll/c820cc12d833db2e3d1e
---Upgraded to v5 OMOP
---INTERVAL set to 30 days

---Chris Knoll's comments are after two dashes
---Taylor Delehanty's comments are after three dashes
---Daniil Terentyev's comments are after four dashes
---proper schema name needs to replace "<schema>" in the code
---operates with a system that auto-generates condition_era_id
---can filter out unmapped condition_concept_id's /*see comment in code*/
--------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS <schema>.tmp_condition_target;
---- Step zero. Prepare data for calculation of condition_era table
CREATE TEMPORARY TABLE <schema>.tmp_condition_target AS
    SELECT
        co.condition_occurrence_id,
        co.person_id,
        co.condition_concept_id,
        co.condition_start_date,
        COALESCE(co.condition_end_date, condition_start_date + INTERVAL '1 day') AS condition_end_date
    FROM
        <schema>.condition_occurrence co
    /* Depending on the needs of your data, you can put more filters on to your code. We assign 0 to our unmapped condition_concept_id's,
     * and since we don't want different conditions put in the same era, we put in the filter below.
      */
    ---WHERE condition_concept_id != 0
;

DROP TABLE IF EXISTS <schema>.tmp_condition_era_s1;
---- Step one. Assign 0 or 1 for each period if it's overlapped by one of previous
CREATE TEMPORARY TABLE <schema>.tmp_condition_era_s1 AS
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date,
        condition_end_date,
        condition_occurrence_id,                                            
        CASE
            WHEN
                DATEDIFF(MAX(condition_end_date) OVER (PARTITION BY person_id, condition_concept_id ORDER BY condition_start_date, condition_end_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ), condition_start_date) <= 30 ---- Here you can set number of allowed days between periods
                THEN 0
            ELSE 1
        END AS np_flag
    FROM
        <schema>.tmp_condition_target
;

DROP TABLE IF EXISTS <schema>.tmp_condition_era_s2;
---- Step two. Enumerate periods by summing flags
CREATE TEMPORARY TABLE <schema>.tmp_condition_era_s2 AS
    SELECT
        person_id,
        condition_concept_id,
        condition_start_date,
        condition_end_date,
        condition_occurrence_id,
        SUM(np_flag) OVER (PARTITION BY person_id, condition_concept_id ORDER BY condition_start_date, condition_end_date) AS period_id
    FROM
        <schema>.tmp_condition_era_s1
;

TRUNCATE <schema>.condition_era;
---- Step three. Insert data into condition_era table
INSERT INTO <schema>.condition_era(person_id, condition_concept_id, condition_era_start_date, condition_era_end_date, condition_occurrence_count)
SELECT
    person_id,
    condition_concept_id,
    MIN(condition_start_date)   AS condition_era_start_date,
    MAX(condition_end_date)     AS condition_era_end_date,
    COUNT(*)                    AS condition_occurrence_count
FROM
    <schema>.tmp_condition_era_s2
GROUP BY
    person_id,
    condition_concept_id,
    period_id
;
