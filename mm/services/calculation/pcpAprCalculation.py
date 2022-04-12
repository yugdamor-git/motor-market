from datetime import datetime

class pcpAprCalculation:
    def __init__(self) -> None:
        pass
    
    def apr(self,price,dep):
        loanAmount = price
        totalPayments = 60
        aprn = dep
        aprn = aprn/100
        totalPaymentsPerYear = 12
        compoundPerYear = 1
        interest = pow((1 + aprn/compoundPerYear), compoundPerYear/totalPaymentsPerYear)-1
        value1 = interest * pow((1 + interest), totalPayments)
        value2 = pow((1 + interest), totalPayments) - 1
        pmt    = loanAmount * (value1 / value2)
        pmt = round(pmt,2)
        pmt = str(pmt).split(".")[0]
        return int(pmt)
  
  
    def pcp(self,price,dep):

        # /*****************************PCP Calculation Formula Start From here***********************************/
        rrp = price
        rrp = rrp
        dealer = 0
        partex = 0
        car_price = round(rrp/100) * 100
        percentage = 33.9
        gmfv = round((percentage / 100) * car_price)

        documentFee = 0
        depositAmount = 0
        financeAmount = rrp - depositAmount - dealer - partex

        term = 48
        term = term-1
        apr= dep
        apr= apr/100
        rt1 = round(round(pow(1 + apr, 1 / 12), 6) - 1, 4)

        rate = round((rt1 * 12) / 12, 4)

        nper = term

        pv = round(financeAmount - documentFee / (1 + rate), 4)
        fv = round(-gmfv / (1 + ((pow(1 + apr, 1 / 12) - 1) * 12) / 12), 4)

        pmtn = ""
        pvif = ""

        if rate == 0:
            pmtn = -(pv + fv) / nper
        else:
            pvif = round(pow(0.99998 + rate, term), 4)
            pmtn = round((rate / (pvif - 0.9999)) * -(pv * pvif + fv), 4)
        pmtn = round(pmtn * -1,2)
        return pmtn
        # /*****************************PCP Calculation Formula End here******************************************/
    
    def apr_48(self,price,dep):
        loanAmount = price
        totalPayments = 48
        aprn = dep
        aprn = aprn/100
        totalPaymentsPerYear = 12
        compoundPerYear = 1
        interest = pow((1 + aprn/compoundPerYear), compoundPerYear/totalPaymentsPerYear)-1
        value1 = interest * pow((1 + interest), totalPayments)
        value2 = pow((1 + interest), totalPayments) - 1
        pmt    = loanAmount * (value1 / value2)
        pmt = round(pmt,2)
        pmt = str(pmt).split(".")[0]
        return int(pmt)
  
    def create_and_calc_apr_pcp_variable(self,start,end,offset,function,price):
        final_out = {}
        if function == "apr":
            for i in range(start,end + 1,1):
                percentage = i+offset
                new_percentage = percentage/100
                # percentage_splited = str(percentage).split(".")
                # variable_name = "m_price_{}_{}".format(percentage_splited[0],percentage_splited[1])
                variable_name = str(round(new_percentage,3))
                variable_value = self.apr(price,percentage)
                final_out[variable_name] = variable_value
            final_out["0.399_48"] = self.apr_48(price,39.9)
            final_out["0.499_48"] = self.apr_48(price,49.9)
            final_out["0.299_48"] = self.apr_48(price,29.9)
        
        if function == "pcp":
            for i in range(start,end + 1,1):
                percentage = i+offset
                new_percentage = percentage/100
                # percentage_splited = str(percentage).split(".")
                variable_name = str(round(new_percentage,3))
                variable_value = self.pcp(price,percentage)
                final_out[variable_name] = variable_value
            final_out["0.399_48"] = self.pcp(price,39.9)
            final_out["0.499_48"] = self.pcp(price,49.9)
            final_out["0.299_48"] = self.pcp(price,29.9)
        return final_out
        
    
    def calculate_apr_pcp(self,price,mileage,built):
        current_year = datetime.now().year - 4
        final_output = None
        start = 6
        end = 49
        offset = 0.9
        if mileage <= 80000 and  built >= current_year:
            final_output = self.create_and_calc_apr_pcp_variable(start,end,offset,"pcp",price)
        else:
            final_output = self.create_and_calc_apr_pcp_variable(start,end,offset,"apr",price)
        
        return final_output