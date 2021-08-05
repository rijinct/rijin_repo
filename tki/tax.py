import constants

def get_tax_for_income(income):
    income_after_flat_ded = income - constants.tax_45_slab
    remainin_tax_amt = income_after_flat_ded * constants.tax_45_remain_each_dol
    #print(remainin_tax_amt)
    total_tax_payble = remainin_tax_amt + constants.tax_flat_45k_120k
    return total_tax_payble


if __name__ == "__main__":

    monthly_wo_tax = constants.b_pay / 12
    fortnt_wo_ded = monthly_wo_tax / 2
    
    fortnt_after_ded = fortnt_wo_ded - constants.tax_normal_py
    weekl_py_after_ded = fortnt_after_ded / 2
    tot_pay_wo_tax = monthly_wo_tax * 8
    tax_an = constants.tax_normal_py * 2 * 12

    print("Actual taxable amount for the current year based on my income:{}".format(get_tax_for_income(tot_pay_wo_tax)))
    print("Taxable income annually as per sal statement:{}".format(tax_an))
    print("Total income without tax:{}".format(tot_pay_wo_tax))
    print(weekl_py_after_ded)
    print(fortnt_after_ded)
    print(fortnt_wo_ded)
    print(monthly_wo_tax)





