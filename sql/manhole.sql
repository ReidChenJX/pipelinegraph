SELECT
    ps_code2,
	manhole_type,
	manhole_style,
	cov_dimen1,
	surface_elev,
	in_roadname,
	out_roadname,
	road_name,
	manhole_category,
	ad_code,
	junc_class
FROM
	PUBLIC.ps_manhole2
WHERE
	ps_code2 IS NOT NULL