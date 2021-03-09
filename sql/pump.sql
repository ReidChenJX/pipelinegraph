SELECT
    ps_code2,
	ps_category,
	ps_category_feat,
	status
FROM
	PUBLIC.ps_pumpstation2
WHERE
	ps_code2 IS NOT NULL