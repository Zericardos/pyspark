caempl  = LOAD 'caempl.csv'  using  PigStorage(',') as (id:chararray, salary:int, firstName:chararray, lastName:chararray, state:chararray, department:chararray , date:chararray, gender:chararray, country:chararray, expenditure:double);
dump caempl;

g = GROUP caempl BY (state, department, gender);
dump g;
/*
((CA,FA,M),{(2,123234872,_\d89e,Rogers,CA,FA,20230125,M,Butte,5099.380859),(3,123234874,_\d89,Rogers,CA,FA,20230124,M,Butte,5501.95459)})
((CA,FE,M),{(11,631231482,David,Smith,CA,FE,20230124,M,Merced,4547.692383)})
((CA,FI,F),{(15,845657246,Kumar,Swamy,CA,FI,20230129,F,Tulare,5621.456055)})
((CC,FA,M),{(13,745685214,Eric,Goldsmith,CC,FA,20230124,M,Tulare,6567.149414)})
((CC,FE,M),{(4,123234873,_89,Rogers,CC,FE,20230124,M,Butte,7101.831055)})
((CC,FI,M),{(1,123234878,_\ddd,Rogers,CC,FI,20230124,M,Alameda,6384.911133),(10,546523478,John,Doe,CC,FI,20230124,M,Sacramento,5036.211426)})
((CD,FA,M),{(12,654873219,Zacary,Efron,CD,FA,20230127,M,Fresno,5447.345215)})
((MG,FI,M),{(7,326587417,Joe,Stevens,MG,FI,20230124,M,San Joaquin,5253.331055)})
((PR,FA,F),{(9,332569843,George,ODonnell,PR,FA,20230127,F,Fresno,5355.54834)})
((SC,FA,F),{(5,152934485,Anand,Manikutty,SC,FA,20230127,F,Butte,5235.987793),(14,845657245,Elizabeth,Doe,SC,FA,20230129,F,Tulare,4818.612793)})
((SC,FA,M),{(6,222364883,Carol,Smith,SC,FA,20230124,M,Fresno,5580.146973)})
((SE,FE,F),{(8,332154719,Mary-Anne,Foster,SE,FE,20230129,F,Kern,4565.746094)})
((state,department,gender),{(id,,firstname,lastname,state,department,date,gender,county,)})
 */
d = FOREACH (GROUP caempl BY (state, department, gender)) {a = ORDER caempl BY date ASC;
oldest = LIMIT a 1;
GENERATE FLATTEN(oldest);};
DUMP d;
/*
(3,123234874,_\d89,Rogers,CA,FA,20230124,M,Butte,5501.95459)
(11,631231482,David,Smith,CA,FE,20230124,M,Merced,4547.692383)
(15,845657246,Kumar,Swamy,CA,FI,20230129,F,Tulare,5621.456055)
(13,745685214,Eric,Goldsmith,CC,FA,20230124,M,Tulare,6567.149414)
(4,123234873,_89,Rogers,CC,FE,20230124,M,Butte,7101.831055)
(1,123234878,_\ddd,Rogers,CC,FI,20230124,M,Alameda,6384.911133)
(12,654873219,Zacary,Efron,CD,FA,20230127,M,Fresno,5447.345215)
(7,326587417,Joe,Stevens,MG,FI,20230124,M,San Joaquin,5253.331055)
(9,332569843,George,ODonnell,PR,FA,20230127,F,Fresno,5355.54834)
(5,152934485,Anand,Manikutty,SC,FA,20230127,F,Butte,5235.987793)
(6,222364883,Carol,Smith,SC,FA,20230124,M,Fresno,5580.146973)
(8,332154719,Mary-Anne,Foster,SE,FE,20230129,F,Kern,4565.746094)
(id,,firstname,lastname,state,department,date,gender,county,)
 */