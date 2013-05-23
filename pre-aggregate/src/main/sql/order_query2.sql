drop table if exists _ipfx_test;
create temporary table _ipfx_test (
	con_id int,
	id int,
	l0 int,
	i0 int,
	soff int,
	eoff int);
insert into _ipfx_test values (connection_id(), 1,8,1,3,136);
insert into _ipfx_test values (connection_id(),2,8,4,75,15);
insert into _ipfx_test values (connection_id(),3,8,4,195,199);
insert into _ipfx_test values (connection_id(),4,8,6,11,79);
insert into _ipfx_test values (connection_id(),5,8,8,131,8);
insert into _ipfx_test values (connection_id(),6,8,8,204,143);
insert into _ipfx_test values (connection_id(),7,8,10,67,71);
insert into _ipfx_test values (connection_id(),8,8,12,139,207);
insert into _ipfx_test values (connection_id(),9,8,13,3,135);
insert into _ipfx_test values (connection_id(),10,8,16,75,15);
insert into _ipfx_test values (connection_id(),11,8,16,195,199);
insert into _ipfx_test values (connection_id(),12,8,17,11,79);
insert into _ipfx_test values (connection_id(),13,8,20,131,7);
insert into _ipfx_test values (connection_id(),14,8,20,203,143);
insert into _ipfx_test values (connection_id(),15,8,21,67,71);
insert into _ipfx_test values (connection_id(),16,8,24,139,207);
insert into _ipfx_test values (connection_id(),17,8,24,3,135);
insert into _ipfx_test values (connection_id(),18,8,26,75,15);

select I.id, DD.pegel
from 
	( select A.id, A.soff,D.cnt,A.eoff,
		 _ipfx_mediumblobToInt(substring(median_start,(A.soff-1)*3+1, 3)) as start_cnt,
		 hex(substring(D.median_start,(A.soff-1)*3+1, 3)),
		 _ipfx_mediumblobToInt(substring(median_start,(D.cnt-A.eoff-1)*3+1, 3)) as end_cnt,
		 hex(substring(D.median_start,(D.cnt-A.eoff-1)*3+1, 3)),
		 D.base_id+round(D.cnt/2)+_ipfx_mediumblobToInt(substring(median_start,(A.soff-1)*3+1, 3))+_ipfx_mediumblobToInt(substring(median_start,(D.cnt-A.eoff-1)*3+1, 3)) as med_pos
		 from pegel_andelfingen2_pa_order_mediumblob D,
		 		_ipfx_test A
	where D.i0=A.i0 and connection_id()=A.con_id) I,
	pegel_andelfingen2 DD
where I.med_pos=DD.ID;

select * from _ipfx_test;

