/* THIS CODE IS NOT FULLY TESTED YET. It still needs to be verified, but it uses the same
 * algorithm found in Chris_Knoll's script: https://gist.github.com/chrisknoll/c820cc12d833db2e3d1e
 */

--------------------------------------------------------------------------------------------------------------
---Adapted to PostgreSQL v5 dose_era from Pure SQL drug_era written by Chris_Knoll: https://gist.github.com/chrisknoll/c820cc12d833db2e3d1e
---INTERVAL set to 30 days
 
---Chris Knoll's comments are after two dashes
---Taylor Delehanty's comments are after three dashes
---proper schema name needs to replace "<schema>" in the code
---proper schema name needs to replace "<vocabulary_schema>". This schema is where the vocabularies and concepts are located.
---works with dose_era_id being self-generated
--------------------------------------------------------------------------------------------------------------

TRUNCATE <schema>.dose_era;

WITH cteDrugTarget(drug_exposure_id, person_id, ingredient_concept_id, unit_concept_id, dose_value, drug_exposure_start_date, days_supply, drug_exposure_end_date) AS
(
	SELECT
		d.drug_exposure_id
		, d.person_id
		, c.concept_id AS ingredient_concept_id
		, d.dose_unit_concept_id AS unit_concept_id
		, d.effective_drug_dose AS dose_value
		, d.drug_exposure_start_date
		, d.days_supply AS days_supply
		, COALESCE(NULLIF(drug_exposure_end_date, NULL), NULLIF(drug_exposure_start_date + (INTERVAL '1 day' * days_supply), drug_exposure_start_date), drug_exposure_start_date + INTERVAL '1 day') AS drug_exposure_end_date
	FROM <schema>.drug_exposure d
	     JOIN <vocabulary_schema>.concept_ancestor ca ON ca.descendant_concept_id = d.drug_concept_id
	     JOIN <vocabulary_schema>.concept c ON ca.ancestor_concept_id = c.concept_id
	     WHERE c.vocabulary_id = 8
	     AND c.concept_class = 'Ingredient'
	     /* Depending on the needs of your data, you can put more filters on to your code. We assign 0 to unmapped drug_concept_id's, and we found data where days_supply was negative.
	      * We don't want different drugs put in the same era, so the code below shows how we filtered them out.
	      * We also don't want negative days_supply, because that will pull our end_date before the start_date due to our second parameter in the COALESCE function.
	      * For now, we are filtering those out as well, but this is a data quality issue that we are trying to solve.
	      */
	     ---AND d.drug_concept_id != 0
	     ---AND d.days_supply >= 0
)
-----------------------------------------------------------------------------------------------------------------------------
, cteEndDates(person_id, ingredient_concept_id, unit_concept_id, dose_value, end_date) AS 
(
	SELECT
		person_id
		, ingredient_concept_id
		, unit_concept_id
		, dose_value
		, event_date - INTERVAL '30 days' AS end_date
	FROM
	(
		SELECT
			person_id
			, ingredient_concept_id
			, unit_concept_id
			, dose_value
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value ORDER BY event_date, event_type ROWS unbounded preceding) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id, unit_concept_id, dose_value ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, ingredient_concept_id
				, unit_concept_id
				, dose_value
				, drug_exposure_start_date AS event_date
				, -1 AS event_type, ROW_NUMBER() OVER(PARTITION BY person_id, drug_concept_id, dose_unit_concept_id, effective_drug_dose ORDER BY drug_exposure_start_date) AS start_ordinal
			FROM cteDrugTarget

			UNION ALL

			SELECT
				person_id
				, ingredient_concept_id
				, unit_concept_id
				, dose_value
				, drug_exposure_end_date + INTERVAL '30 days'
				, 1 AS event_type
				, NULL
			FROM cteDrugTarget
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
)
-----------------------------------------------------------------------------------------------------------------------------
, cteDoseEraEnds(person_id, drug_concept_id, unit_concept_id, dose_value, drug_exposure_start_date, dose_era_end_date) AS
( SELECT
	dt.person_id
	, dt.ingredient_concept_id
	, dt.unit_concept_id
	, dt.dose_value
	, dt.drug_exposure_start_date
	, MIN(e.end_date) AS era_end_date
FROM cteDrugTarget dt
JOIN cteEndDates e
ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND dt.unit_concept_id = e.unit_concept_id AND dt.dose_value = e.dose_value AND e.end_date >= dt.drug_exposure_start_date
GROUP BY
	dt.drug_exposure_id
	, dt.person_id
	, dt.ingredient_concept_id
	, dt.unit_concept_id
	, dt.dose_value
	, dt.drug_exposure_start_date
-----------------------------------------------------------------------------------------------------------------------------
INSERT INTO <schema>.dose_era(person_id, drug_concept_id, unit_concept_id, dose_value, dose_era_start_date, dose_era_end_date)
SELECT
	person_id
	, drug_concept_id
	, unit_concept_id
	, dose_value
	, MIN(drug_exposure_start_date) AS dose_era_start_date
	, dose_era_end_date
GROUP BY person_id, drug_concept_id, unit_concept_id, dose_value, dose_era_end_date
ORDER BY person_id, drug_concept_id
;
