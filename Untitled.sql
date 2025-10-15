select * 
from OPENLAKE_RAW."rep_psgs"."transactions"
where "transaction_id" > 5100000
;

select * 
from OPENLAKE_ICE."openlakeslv"."transactions"
where "transaction_id" > 5100000
;