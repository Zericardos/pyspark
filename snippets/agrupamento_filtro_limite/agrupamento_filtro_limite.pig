-- Usos: re-or-se-e-h
caempl  = LOAD 'caempl2.csv'  using  PigStorage(',') as (id:chararray, salary:int, firstName:chararray, lastName:chararray, state:chararray, department:chararray , initdate:chararray, enddate:chararray, gender:chararray, country:chararray, expenditure:double);
dump caempl;

g = GROUP caempl BY state;
dump g;

d = foreach g  {
			venda = filter caempl by UPPER(department) == 'FA';
			first = limit caempl 1;
			generate
			group as state,
            flatten (first.salary) as salary,
			flatten (first.firstName) as firstName,
			flatten (first.lastName) as lastName,
			flatten (first.department) as department,
			flatten (first.id) as id,
			COUNT(venda) as typecount;
			};
dump d;
/*
(CA,123234874,_\d89,Rogers,FA,3,2)
(CC,745685214,Eric,Goldsmith,FA,13,1)
(CD,654873219,Zacary,Efron,FA,12,1)
(MG,326587417,Joe,Stevens,FI,7,0)
(PR,332569843,George,ODonnell,FA,9,1)
(SC,578975482,Festa,Eduardo,FA,27,15)
(SE,332154719,Mary-Anne,Foster,FE,8,0)
(state,,firstname,lastname,department,id,0)
*/
e = foreach g  {
			venda = filter caempl by UPPER(department) == 'FA';
			first = limit caempl 1;
			generate
			group as state,
            flatten (first.salary) as salary,
			flatten (first.firstName) as firstName,
			flatten (first.lastName) as lastName,
			flatten (first.department) as department,
			flatten (first.id) as id,
			COUNT(venda) as typecount;
			};
dump e;
/*
(CA,123234874,_\d89,Rogers,FA,3,2)
(CC,745685214,Eric,Goldsmith,FA,13,1)
(CD,654873219,Zacary,Efron,FA,12,1)
(MG,326587417,Joe,Stevens,FI,7,0)
(PR,332569843,George,ODonnell,FA,9,1)
(SC,578975482,Festa,Eduardo,FA,27,15)
(SE,332154719,Mary-Anne,Foster,FE,8,0)
(state,,firstname,lastname,department,id,0)
 */