caempl  = LOAD 'caempl2.csv'  using  PigStorage(',') as (id:chararray, salary:int, firstName:chararray, lastName:chararray, state:chararray, department:chararray , initdate:chararray, enddate:chararray, gender:chararray, country:chararray, expenditure:double);
dump caempl;

g = GROUP caempl BY (state, department, gender);
dump g;
/*
((CA,FA,M),{(3,123234874,_\d89,Rogers,CA,FA,20103112,20230124,M,Butte,5501.95459),(2,123234872,_\d89e,Rogers,CA,FA,20103111,20230125,M,Butte,5099.380859)})
((CA,FE,M),{(11,631231482,David,Smith,CA,FE,20103116,20230124,M,Merced,4547.692383)})
((CA,FI,F),{(15,845657246,Kumar,Swamy,CA,FI,20103110,20230129,F,Tulare,5621.456055)})
((CC,FA,M),{(13,745685214,Eric,Goldsmith,CC,FA,20103110,20230124,M,Tulare,6567.149414)})
((CC,FE,M),{(4,123234873,_89,Rogers,CC,FE,20104110,20230124,M,Butte,7101.831055)})
((CC,FI,M),{(1,123234878,_\ddd,Rogers,CC,FI,20103110,20230124,M,Alameda,6384.911133),(10,546523478,John,Doe,CC,FI,20103116,20230124,M,Sacramento,5036.211426)})
((CD,FA,M),{(12,654873219,Zacary,Efron,CD,FA,20103116,20230127,M,Fresno,5447.345215)})
((MG,FI,M),{(7,326587417,Joe,Stevens,MG,FI,20103110,20230124,M,San Joaquin,5253.331055)})
((PR,FA,F),{(9,332569843,George,ODonnell,PR,FA,20103116,20230127,F,Fresno,5355.54834)})
((SC,FA,F),{(21,214548783,Gerusa,Albuquerqu,SC,FA,20090106,20230126,F,Sacramento,5872.5898722),(17,897123057,Joaquina,Carlota,SC,FA,20090106,20230125,F,Sacramento,5872.5898722),(14,845657245,Elizabeth,Doe,SC,FA,20103110,20230129,F,Tulare,4818.612793),(26,578975482,Debora,Tavares,SC,FA,20360809,20230125,F,Sacramento,5872.5898722),(5,152934485,Anand,Manikutty,SC,FA,20104110,20230127,F,Butte,5235.987793),(24,578975482,Laura,Martins,SC,FA,20160809,20230122,F,Sacramento,5872.5898722),(25,578975482,Larissa,Martins,SC,FA,20360809,20230125,F,Sacramento,5872.5898722)})
((SC,FA,M),{(27,578975482,Festa,Eduardo,SC,FA,20360809,20230125,M,Sacramento,5872.5898722),(23,578975482,Jung,Bugre,SC,FA,20090106,20230122,M,Sacramento,5872.5898722),(22,578975482,Pedro,Sanson,SC,FA,20090106,20230122,M,Sacramento,5872.5898722),(20,897123057,Franck,Nuncio,SC,FA,20090106,20230126,M,Sacramento,5872.5898722),(19,897123057,Tadeu,Schimidt,SC,FA,20090106,20230126,M,Sacramento,5872.5898722),(18,897123057,Jose,Ricardo,SC,FA,20090106,20230126,M,Sacramento,5872.5898722),(16,158787946,Joao,Borges,SC,FA,20090106,20230124,M,Tulare,7821.57892),(6,222364883,Carol,Smith,SC,FA,20103115,20230124,M,Fresno,5580.146973)})
((SE,FE,F),{(8,332154719,Mary-Anne,Foster,SE,FE,20103115,20230129,F,Kern,4565.746094)})
((state,department,gender),{(id,,firstname,lastname,state,department,initdate,enddate,gender,county,)})
 */
d = FOREACH (GROUP caempl BY (state, department, gender)) {a = ORDER caempl BY initdate,enddate;
oldest = LIMIT a 1;
GENERATE FLATTEN(oldest);};
DUMP d;
/*
(2,123234872,_\d89e,Rogers,CA,FA,20103111,20230125,M,Butte,5099.380859)
(11,631231482,David,Smith,CA,FE,20103116,20230124,M,Merced,4547.692383)
(15,845657246,Kumar,Swamy,CA,FI,20103110,20230129,F,Tulare,5621.456055)
(13,745685214,Eric,Goldsmith,CC,FA,20103110,20230124,M,Tulare,6567.149414)
(4,123234873,_89,Rogers,CC,FE,20104110,20230124,M,Butte,7101.831055)
(1,123234878,_\ddd,Rogers,CC,FI,20103110,20230124,M,Alameda,6384.911133)
(12,654873219,Zacary,Efron,CD,FA,20103116,20230127,M,Fresno,5447.345215)
(7,326587417,Joe,Stevens,MG,FI,20103110,20230124,M,San Joaquin,5253.331055)
(9,332569843,George,ODonnell,PR,FA,20103116,20230127,F,Fresno,5355.54834)
(17,897123057,Joaquina,Carlota,SC,FA,20090106,20230125,F,Sacramento,5872.5898722)
(22,578975482,Pedro,Sanson,SC,FA,20090106,20230122,M,Sacramento,5872.5898722)
(8,332154719,Mary-Anne,Foster,SE,FE,20103115,20230129,F,Kern,4565.746094)
(id,,firstname,lastname,state,department,initdate,enddate,gender,county,)
 */