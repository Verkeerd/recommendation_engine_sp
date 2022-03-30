CREATE TABLE products (
	product__id 				VARCHAR 			CONSTRAINT c_prod_pk_id 			PRIMARY KEY,
	brand 						VARCHAR,
	category 					VARCHAR,
	color 						VARCHAR,
	fast_mover 					VARCHAR				CONSTRAINT c_prod_bool_fm 			CHECK (fast_mover 			IN ('true', 'false')),
	flavor 						VARCHAR,
	gender 						VARCHAR,
	herhaalaankopen 			VARCHAR 			CONSTRAINT c_prod_bool_ha 			CHECK (herhaalaankopen 		IN ('true', 'false')),
	product_name 				VARCHAR				CONSTRAINT c_prod_nn_name 			NOT NULL,
	selling_price 				INT					CONSTRAINT c_prod_nn_price			NOT NULL,
	availability 				INT,
	discount 					VARCHAR,
	doelgroep 					VARCHAR,
	eenheid 					VARCHAR,
	factor 						VARCHAR,
	folder_actief			 	VARCHAR,
	gebruik 					VARCHAR,
	geschikt_voor 				VARCHAR,
	geursoort 					VARCHAR,
	huidconditie 				VARCHAR,
	huidtype 					VARCHAR,
	huidtypegezicht 			VARCHAR,
	inhoud 						VARCHAR,
	klacht 						VARCHAR,
	kleur 						VARCHAR,
	leeftijd 					VARCHAR,
	mid 						INT,
	online_only 				VARCHAR,
	serie 						VARCHAR,
	shopcart_promo_item 		VARCHAR,
	shopcart_promo_price 		INT,
	soort 						VARCHAR,
	soort_haarverzorging 		VARCHAR,
	soort_mondverzorging 		VARCHAR,
	sterkte 					VARCHAR,
	product_type 				VARCHAR,
	typehaarkleuring 			VARCHAR,
	typetandenbostel 			VARCHAR,
	variant 					VARCHAR,
	waterproof 					VARCHAR,
	weekdeal 					VARCHAR,
	recommendable				VARCHAR 			CONSTRAINT c_prod_bool_rec 			CHECK (recommendable 		IN ('true', 'false')),
	sub_category 				VARCHAR,
	sub_sub_category 			VARCHAR,
	sub_sub_sub_category 		VARCHAR
);

CREATE TABLE profiles(
	profile__id 	VARCHAR							CONSTRAINT c_prof_pk 				PRIMARY KEY,
	created 		TIMESTAMP,
	latest_order 	TIMESTAMP WITH TIME ZONE,
	first_order 	TIMESTAMP WITH TIME ZONE,
	latest_activity TIMESTAMP WITH TIME ZONE
);

CREATE TABLE recommendations(
	recommendation_id 	 SERIAL 					PRIMARY KEY,
	profile__id 		 VARCHAR,
	recommendation_time  TIMESTAMP WITH TIME ZONE 	CONSTRAINT c_r_nn_rt 				NOT NULL,
	segment 			 VARCHAR,
	last_visit 			 TIMESTAMP WITH TIME ZONE,
	total_pageview_count INT,
	total_viewed_count 	 INT,
	CONSTRAINT c_r_fk_pif 							FOREIGN KEY (profile__id)			REFERENCES profiles(profile__id)
);

CREATE TABLE recommendation_products(
	recommendation_id 	BIGINT,
	product__id 		VARCHAR,
	recommendation_type VARCHAR 					CONSTRAINT c_rp_cat_rec CHECK (recommendation_type IN ('viewed', 'similar')),
	CONSTRAINT c_rp_pk 								PRIMARY KEY (recommendation_id, product__id),
	CONSTRAINT c_rp_fk_rid 							FOREIGN KEY (recommendation_id)		REFERENCES recommendations(recommendation_id),
	CONSTRAINT c_rp_fk_pid 							FOREIGN KEY (product__id)			REFERENCES products(product__id)
);

CREATE TABLE buid(
	buid 		VARCHAR 							CONSTRAINT c_b_pk 					PRIMARY KEY,
	profile__id VARCHAR,
	CONSTRAINT c_b_fk_pid 							FOREIGN KEY (profile__id)			REFERENCES profiles(profile__id)
);

CREATE TABLE sessions(
	session__id 		VARCHAR 					CONSTRAINT c_ses_pk 				PRIMARY KEY,
	buid 				VARCHAR,
	session_start 		TIMESTAMP WITH TIME ZONE	CONSTRAINT c_ses_nn_ss 				NOT NULL,
	session_end 		TIMESTAMP WITH TIME ZONE	CONSTRAINT c_ses_nn_se 				NOT NULL,
	has_sale 			VARCHAR 					CONSTRAINT c_ses_bool_hs 			CHECK (has_sale IN ('true', 'false')),
	segment 			VARCHAR,
	CONSTRAINT c_ses_fk_bid 						FOREIGN KEY (buid)					REFERENCES buid(buid)
);

CREATE TABLE events(
	session__id 		VARCHAR,
	previous_events 	INT,
	event_time 			TIMESTAMP WITH TIME ZONE,
	event_source 		VARCHAR,
	event_action 		VARCHAR 						CONSTRAINT c_e_nn_ea 				NOT NULL,
	page_type 			VARCHAR,
	product 			VARCHAR,
	time_on_page 		INT,
	click_count 		INT,
	elements_clicked 	INT,
	scrolls_down 		INT,
	scrolls_up 			INT,
	CONSTRAINT c_e_pk 									PRIMARY KEY (session__id, previous_events),
	CONSTRAINT c_e_fk_sid 								FOREIGN KEY (session__id)			REFERENCES sessions(session__id)
);

CREATE TABLE preferences( -- bools: ; categorical: category
	session__id VARCHAR,
	category 	VARCHAR,
	preference 	VARCHAR 								CONSTRAINT c_pref_nn_p 				NOT NULL,
	views       INT,
	sales       INT,
	CONSTRAINT c_pref_pk 								PRIMARY KEY (session__id, category),
	CONSTRAINT c_pref_fk 								FOREIGN KEY (session__id)			REFERENCES sessions(session__id)
);

CREATE TABLE ordered_products(
	session__id VARCHAR,
	product__id VARCHAR,
	total 		INT,
	CONSTRAINT c_op_pk 									PRIMARY KEY (product__id, session__id),
	CONSTRAINT c_op_fk_pid 								FOREIGN KEY (product__id) 			REFERENCES products(product__id),
	CONSTRAINT c_op_fk_sid 								FOREIGN KEY (session__id) 			REFERENCES sessions(session__id)
);

-- creates a buid for parentless history
INSERT INTO profiles (profile__id) VALUES ('ADMIN');
INSERT INTO buid (buid, profile__id) VALUES ('0', 'ADMIN');

-- trigger that skips all products with a null value for name or price.
CREATE FUNCTION trigger_nn_product()
   RETURNS TRIGGER
   LANGUAGE PLPGSQL
AS $$
BEGIN
	IF (NEW.selling_price IS NULL OR NEW.product_name IS NULL)
		THEN RETURN NULL;
	ELSE
		RETURN NEW;
END IF;
END;
$$;

CREATE TRIGGER products_nn_price
   BEFORE INSERT
   ON products
   FOR EACH ROW
       EXECUTE PROCEDURE trigger_nn_product();
