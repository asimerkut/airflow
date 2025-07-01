CREATE TABLE IF NOT EXISTS public.epikriz_ozet
(
    takip_no numeric NOT NULL,
    ozet_s text COLLATE pg_catalog."default",
    ozet_o text COLLATE pg_catalog."default",
    ozet_a text COLLATE pg_catalog."default",
    ozet_p text COLLATE pg_catalog."default",
    biobert_vector vector(768),
    CONSTRAINT epikriz_ozet_pkey PRIMARY KEY (takip_no)
)



