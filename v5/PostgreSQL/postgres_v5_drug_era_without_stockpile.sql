/* THIS CODE IS NOT FULLY TESTED YET. It still needs to be verified, but it uses the same
 * algorithm found in Chris_Knoll's script: https://gist.github.com/chrisknoll/c820cc12d833db2e3d1e
 */

--------------------------------------------------------------------------------------------------------------
---Adapted to PostgreSQL drug_era from Pure SQL drug_era written by Chris_Knoll: https://gist.github.com/chrisknoll/c820cc12d833db2e3d1e
---Upgraded to v5
---Uses STOCKPILE method to populate gap_days field
---INTERVAL set to 30 days
 
---Chris Knoll's comments are after two dashes
---Taylor Delehanty's comments are after three dashes
---proper schema for "<schema>" needs to be replaced in the code
---proper schema for "<vocabulary_and_concept_schema>" needs to be replaced in the code. This schema is where the vocabularies and concepts are located.
-------------------------------------------------------------------------------------------------------------

---TRUNCATE <schema>.drug_era;

WITH
ctePreDrugTarget(drug_exposure_id, person_id, ingredient_concept_id, drug_exposure_start_date, days_supply, drug_exposure_end_date) AS
(-- Normalize DRUG_EXPOSURE_END_DATE to either the existing drug exposure end date, or add days supply, or add 1 day to the start date
    SELECT
        d.drug_exposure_id
        , d.person_id
        , c.concept_id AS ingredient_concept_id
        , d.drug_exposure_start_date AS drug_exposure_start_date
        , d.days_supply AS days_supply
        , COALESCE(
            NULLIF(drug_exposure_end_date, NULL) ---If drug_exposure_end_date != NULL, return drug_exposure_end_date, otherwise go to next case
            , NULLIF(drug_exposure_start_date + (INTERVAL '1 day' * days_supply), drug_exposure_start_date) ---If days_supply != NULL or 0, return drug_exposure_start_date + days_supply, otherwise go to next case
            , drug_exposure_start_date + INTERVAL '1 day' ---Add 1 day to the drug_exposure_start_date since there is no end_date or INTERVAL for the days_supply
        ) AS drug_exposure_end_date
    FROM <schema>.drug_exposure d
    JOIN <vocabulary_and_concept_schema>.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
    JOIN <vocabulary_and_concept_schema>.concept c ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 8 ---8 selects RxNorm from the vocabulary_id
    AND c.concept_class = 'Ingredient'
    /* Depending on the needs of your data, you can put more filters on to your code. We assign 0 to unmapped drug_concept_id's, and we found data where days_supply was negative.
     * We don't want different drugs put in the same era, so the code below shows how we filtered them out.
      * We also don't want negative days_supply, because that will pull our end_date before the start_date due to our second parameter in the COALESCE function.
      * For now, we are filtering those out as well, but this is a data quality issue that we are trying to solve.
      */
    ---AND d.drug_concept_id != 0
    ---AND d.days_supply >= 0
)

, cteSubExposureEndDates (person_id, ingredient_concept_id, end_date) AS --- A preliminary sorting that groups all of the overlapping exposures into one exposure so that we don't double-count non-gap-days
(
    SELECT
        person_id
        , ingredient_concept_id
        , event_date AS end_date
    FROM
    (
        SELECT
            person_id
            , ingredient_concept_id
            , event_date
            , event_type
            , MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS unbounded preceding) AS start_ordinal -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with
            , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
        FROM (
            -- select the start dates, assigning a row number to each
            SELECT
                person_id
                , ingredient_concept_id
                , drug_exposure_start_date AS event_date
                , -1 AS event_type
                , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_exposure_start_date) AS start_ordinal
            FROM ctePreDrugTarget
        
            UNION ALL

            SELECT
                person_id
                , ingredient_concept_id
                , drug_exposure_end_date
                , 1 AS event_type
                , NULL
            FROM ctePreDrugTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0 
)
    
, cteDrugExposureEnds (person_id, drug_concept_id, drug_exposure_start_date, drug_sub_exposure_end_date) AS
(
    SELECT 
           dt.person_id
           , dt.ingredient_concept_id
           , dt.drug_exposure_start_date
           , MIN(e.end_date) AS drug_sub_exposure_end_date
    FROM ctePreDrugTarget dt
    JOIN cteSubExposureEndDates e ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
    GROUP BY 
                dt.drug_exposure_id
                , dt.person_id
          , dt.ingredient_concept_id
          , dt.drug_exposure_start_date
)
--------------------------------------------------------------------------------------------------------------
, cteSubExposures(row_number, person_id, drug_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count) AS
(
    SELECT
        ROW_NUMBER() OVER (PARTITION BY person_id, drug_concept_id, drug_sub_exposure_end_date)
        , person_id
        , drug_concept_id
        , MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date
        , drug_sub_exposure_end_date
        , COUNT(*) AS drug_exposure_count
    FROM cteDrugExposureEnds
    GROUP BY person_id, drug_concept_id, drug_sub_exposure_end_date
    ORDER BY person_id, drug_concept_id
)
--------------------------------------------------------------------------------------------------------------
/*Everything above grouped exposures into sub_exposures if there was overlap between exposures.
 *This means no persistence window was implemented. Now we CAN add the persistence window to calculate eras.
 */
--------------------------------------------------------------------------------------------------------------
, cteFinalTarget(row_number, person_id, ingredient_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count, days_exposed) AS
(
    SELECT
        row_number
        , person_id
        , drug_concept_id
        , drug_sub_exposure_start_date
        , drug_sub_exposure_end_date
        , drug_exposure_count
        , drug_sub_exposure_end_date - drug_sub_exposure_start_date AS days_exposed
    FROM cteSubExposures
)
--------------------------------------------------------------------------------------------------------------
, cteEndDates (person_id, ingredient_concept_id, end_date) AS -- the magic
(
    SELECT
        person_id
        , ingredient_concept_id
        , event_date - INTERVAL '30 days' AS end_date -- unpad the end date
    FROM
    (
        SELECT
            person_id
            , ingredient_concept_id
            , event_date
            , event_type
            , MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with
            , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY event_date, event_type) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
        FROM (
            -- select the start dates, assigning a row number to each
            SELECT
                person_id
                , ingredient_concept_id
                , drug_sub_exposure_start_date AS event_date
                , -1 AS event_type
                , ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id ORDER BY drug_sub_exposure_start_date) AS start_ordinal
            FROM cteFinalTarget
        
            UNION ALL
        
            -- pad the end dates by 30 to allow a grace period for overlapping ranges.
            SELECT
                person_id
                , ingredient_concept_id
                , drug_sub_exposure_end_date + INTERVAL '30 days'
                , 1 AS event_type
                , NULL
            FROM cteFinalTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0 

)
, cteDrugEraEnds (person_id, drug_concept_id, drug_sub_exposure_start_date, drug_era_end_date, drug_exposure_count, days_exposed) AS
(
SELECT 
    ft.person_id
    , ft.ingredient_concept_id
    , ft.drug_sub_exposure_start_date
    , MIN(e.end_date) AS era_end_date
    , ft.drug_exposure_count
    , ft.days_exposed
FROM cteFinalTarget ft
JOIN cteEndDates e ON ft.person_id = e.person_id AND ft.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= ft.drug_sub_exposure_start_date
GROUP BY 
          ft.person_id
    , ft.ingredient_concept_id
    , ft.drug_sub_exposure_start_date
    , ft.drug_exposure_count
    , ft.days_exposed
)
INSERT INTO <schema>.drug_era(person_id, drug_concept_id, drug_era_start_date, drug_era_end_date, drug_exposure_count, gap_days)
SELECT
    person_id
    , drug_concept_id
    , MIN(drug_sub_exposure_start_date) AS drug_era_start_date
    , drug_era_end_date
    , SUM(drug_exposure_count) AS drug_exposure_count
    , EXTRACT(EPOCH FROM drug_era_end_date - MIN(drug_sub_exposure_start_date) - SUM(days_exposed)) / 86400 AS gap_days
FROM cteDrugEraEnds
GROUP BY person_id, drug_concept_id, drug_era_end_date
ORDER BY person_id, drug_concept_id

/*
SELECT
    (
        SELECT SUM(drug_exposure_count) FROM cteFinalTarget
    ) AS sum
    , (
        SELECT COUNT(*) FROM <schema>.drug_exposure d
            JOIN <vocabulary_and_concept_schema>.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
            JOIN <vocabulary_and_concept_schema>.concept c ON ca.ancestor_concept_id = c.concept_id
            WHERE c.vocabulary_id = 8
            AND c.concept_class = 'Ingredient'
            AND d.drug_concept_id != 0
            AND d.days_supply >= 0
    ) AS count
*/
