############################################################
# As a foreign resident, you fall under the slab as below
#
#Taxable income
#=======================================================================
#        0 – $120,000 | 32.5 cents for each $1
# $120,001 – $180,000 | $39,000 plus 37 cents for each $1 over $120,000
# $180,001   and over | $61,200 plus 45 cents for each $1 over $180,000
#=======================================================================
### A normal PR guy falls under normal slab and the tax is 10K lesser
#
#
############################################################

import constants

def get_tax_for_income(income):
    income_beyond_120K_slab = income - constants.tax_120_slab_f
    remainin_tax_amt = income_beyond_120K_slab * constants.tax_perc_120_180K_f
    #print(remainin_tax_amt)
    total_tax_payble = remainin_tax_amt + constants.tax_flat_120_180K
    return total_tax_payble


if __name__ == "__main__":

    total_tax = get_tax_for_income(constants.b_pay_f)
    income_after_tax_annually = constants.b_pay_f - total_tax
    income_after_tax_monthly = income_after_tax_annually / 12

    print("Actual taxable amount for the current year based on my income:{}".format(total_tax))
    print("Total income after tax deduction annually:{}".format(income_after_tax_annually))
    print("Total income after tax deduction Monthly:{}".format(income_after_tax_monthly))






