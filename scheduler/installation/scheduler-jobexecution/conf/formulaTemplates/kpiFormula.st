case  
$formula_obj_list:{ formula_obj | when $formula_obj.dimAndValList:{ dimAndVal | $dimAndVal$ };separator=" and "$ then
(
$formula_obj.weightedAvgFormula$
)}$
else
$defaultFormula$
end
