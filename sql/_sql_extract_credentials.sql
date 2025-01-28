SELECT
	I."user",
	I."password",
	II.description,
	II.id_origin
FROM bbdd_kreton_learning.sch_itl.tb_credentials I
INNER JOIN bbdd_kreton_learning.sch_itl.tb_origins II
	ON I.id_origin  = II.id_origin
WHERE I.active = True