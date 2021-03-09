SELECT
	ps_code2,
	pipe_level,
	pipe_category,
	pressure_type,
	shapetype,
	material,
	constr_method,
	rconstr_method,
	status,
	road_name,
	in_roadname,
	out_roadname,
	ad_code
FROM
	PUBLIC.ps_pipe2
WHERE
	ps_code2 IS NOT NULL