
from common_config import margin_cc_dict



class price_conditions:
    def __init__(self):
        
        print("price conditions init")

        car_types_dict = {'Alfa Romeo;2000': {'category': 'luxury',
        'make': 'Alfa Romeo',
        'model': '2000'},
        'Alfa Romeo;4C': {'category': 'luxury', 'make': 'Alfa Romeo', 'model': '4C'},
        'Alfa Romeo;Stelvio': {'category': '4x4',
        'make': 'Alfa Romeo',
        'model': 'Stelvio'},
        'Audi;Q2': {'category': '4x4', 'make': 'Audi', 'model': 'Q2'},
        'Audi;Q5': {'category': 'luxury', 'make': 'Audi', 'model': 'Q5'},
        'Audi;Q7': {'category': 'luxury', 'make': 'Audi', 'model': 'Q7'},
        'Audi;QUATTRO': {'category': 'luxury', 'make': 'Audi', 'model': 'QUATTRO'},
        'Audi;RS Q3': {'category': 'luxury', 'make': 'Audi', 'model': 'RS Q3'},
        'Audi;RS3': {'category': 'luxury', 'make': 'Audi', 'model': 'RS3'},
        'Audi;RS4': {'category': 'luxury', 'make': 'Audi', 'model': 'RS4'},
        'Audi;RS5': {'category': 'luxury', 'make': 'Audi', 'model': 'RS5'},
        'Audi;RS6': {'category': 'luxury', 'make': 'Audi', 'model': 'RS6'},
        'Audi;RS7': {'category': 'luxury', 'make': 'Audi', 'model': 'RS7'},
        'Audi;S3': {'category': 'luxury', 'make': 'Audi', 'model': 'S3'},
        'Audi;S4': {'category': 'luxury', 'make': 'Audi', 'model': 'S4'},
        'Audi;S5': {'category': 'luxury', 'make': 'Audi', 'model': 'S5'},
        'Audi;S6 AVANT': {'category': 'luxury', 'make': 'Audi', 'model': 'S6 AVANT'},
        'Audi;S7': {'category': 'luxury', 'make': 'Audi', 'model': 'S7'},
        'Audi;S8': {'category': 'luxury', 'make': 'Audi', 'model': 'S8'},
        'Audi;SQ5': {'category': 'luxury', 'make': 'Audi', 'model': 'SQ5'},
        'BMW Alpina;B10': {'category': 'luxury',
        'make': 'BMW Alpina',
        'model': 'B10'},
        'BMW;1-Series': {'category': 'luxury', 'make': 'BMW', 'model': '1-Series'},
        'BMW;2-Series': {'category': 'luxury', 'make': 'BMW', 'model': '2-Series'},
        'BMW;2002': {'category': 'luxury', 'make': 'BMW', 'model': '2002'},
        'BMW;M2': {'category': 'luxury', 'make': 'BMW', 'model': 'M2'},
        'BMW;M3': {'category': 'luxury', 'make': 'BMW', 'model': 'M3'},
        'BMW;M4': {'category': 'luxury', 'make': 'BMW', 'model': 'M4'},
        'BMW;M5': {'category': 'luxury', 'make': 'BMW', 'model': 'M5'},
        'BMW;M6': {'category': 'luxury', 'make': 'BMW', 'model': 'M6'},
        'BMW;X1': {'category': '4x4', 'make': 'BMW', 'model': 'X1'},
        'BMW;X2': {'category': '4x4', 'make': 'BMW', 'model': 'X2'},
        'BMW;X3': {'category': '4x4', 'make': 'BMW', 'model': 'X3'},
        'BMW;X4': {'category': 'luxury', 'make': 'BMW', 'model': 'X4'},
        'BMW;X5': {'category': 'luxury', 'make': 'BMW', 'model': 'X5'},
        'BMW;X6': {'category': 'luxury', 'make': 'BMW', 'model': 'X6'},
        'BMW;X7': {'category': 'luxury', 'make': 'BMW', 'model': 'X7'},
        'BMW;Z4': {'category': 'luxury', 'make': 'BMW', 'model': 'Z4'},
        'BMW;i3': {'category': 'luxury', 'make': 'BMW', 'model': 'i3'},
        'Cadillac;BLS': {'category': '4x4', 'make': 'Cadillac', 'model': 'BLS'},
        'Cadillac;Escalade': {'category': '4x4',
        'make': 'Cadillac',
        'model': 'Escalade'},
        'Caterham;Super Seven': {'category': 'luxury',
        'make': 'Caterham',
        'model': 'Super Seven'},
        'Chevrolet;Captiva': {'category': '4x4',
        'make': 'Chevrolet',
        'model': 'Captiva'},
        'Chevrolet;Corvette': {'category': 'luxury',
        'make': 'Chevrolet',
        'model': 'Corvette'},
        'Dodge;Avenger': {'category': '4x4', 'make': 'Dodge', 'model': 'Avenger'},
        'Dodge;CHALLENGER': {'category': 'luxury',
        'make': 'Dodge',
        'model': 'CHALLENGER'},
        'Dodge;CHARGER': {'category': '4x4', 'make': 'Dodge', 'model': 'CHARGER'},
        'Dodge;Caliber': {'category': '4x4', 'make': 'Dodge', 'model': 'Caliber'},
        'Dodge;Nitro': {'category': '4x4', 'make': 'Dodge', 'model': 'Nitro'},
        'Dodge;Ram': {'category': '4x4', 'make': 'Dodge', 'model': 'Ram'},
        'FORD;RANGER': {'category': '4x4', 'make': 'FORD', 'model': 'RANGER'},
        'Ford;Edge': {'category': '4x4', 'make': 'Ford', 'model': 'Edge'},
        'Ford;Mustang': {'category': 'luxury', 'make': 'Ford', 'model': 'Mustang'},
        'Hummer;H2': {'category': 'luxury', 'make': 'Hummer', 'model': 'H2'},
        'Hummer;H3': {'category': 'luxury', 'make': 'Hummer', 'model': 'H3'},
        'Infiniti;Q30': {'category': 'luxury', 'make': 'Infiniti', 'model': 'Q30'},
        'Infiniti;Q50': {'category': 'luxury', 'make': 'Infiniti', 'model': 'Q50'},
        'Infiniti;Q60': {'category': 'luxury', 'make': 'Infiniti', 'model': 'Q60'},
        'Infiniti;Q70': {'category': 'luxury', 'make': 'Infiniti', 'model': 'Q70'},
        'Infiniti;QX50': {'category': 'luxury', 'make': 'Infiniti', 'model': 'QX50'},
        'Infiniti;QX70': {'category': 'luxury', 'make': 'Infiniti', 'model': 'QX70'},
        'Jaguar;E-Pace': {'category': 'luxury', 'make': 'Jaguar', 'model': 'E-Pace'},
        'Jaguar;F-Pace': {'category': 'luxury', 'make': 'Jaguar', 'model': 'F-Pace'},
        'Jaguar;I-Pace': {'category': 'luxury', 'make': 'Jaguar', 'model': 'I-Pace'},
        'Jaguar;S-Type': {'category': 'luxury', 'make': 'Jaguar', 'model': 'S-Type'},
        'Jaguar;X-Type': {'category': 'luxury', 'make': 'Jaguar', 'model': 'X-Type'},
        'Jaguar;XE': {'category': 'luxury', 'make': 'Jaguar', 'model': 'XE'},
        'Jaguar;XF': {'category': 'luxury', 'make': 'Jaguar', 'model': 'XF'},
        'Jaguar;XJ': {'category': 'luxury', 'make': 'Jaguar', 'model': 'XJ'},
        'Jaguar;XJ220': {'category': 'luxury', 'make': 'Jaguar', 'model': 'XJ220'},
        'Jaguar;XJR-S': {'category': 'luxury', 'make': 'Jaguar', 'model': 'XJR-S'},
        'Jaguar;XK': {'category': 'luxury', 'make': 'Jaguar', 'model': 'XK'},
        'Jaguar;XK8': {'category': 'luxury', 'make': 'Jaguar', 'model': 'XK8'},
        'Jaguar;XKR': {'category': 'luxury', 'make': 'Jaguar', 'model': 'XKR'},
        'Jeep;Cherokee': {'category': '4x4', 'make': 'Jeep', 'model': 'Cherokee'},
        'Jeep;Commander': {'category': '4x4', 'make': 'Jeep', 'model': 'Commander'},
        'Jeep;Compass': {'category': '4x4', 'make': 'Jeep', 'model': 'Compass'},
        'Jeep;Grand Cherokee': {'category': '4x4',
        'make': 'Jeep',
        'model': 'Grand Cherokee'},
        'Jeep;Patriot': {'category': '4x4', 'make': 'Jeep', 'model': 'Patriot'},
        'Jeep;Renegade': {'category': '4x4', 'make': 'Jeep', 'model': 'Renegade'},
        'Jeep;Wrangler': {'category': '4x4', 'make': 'Jeep', 'model': 'Wrangler'},
        'Land Rover;110': {'category': 'luxury',
        'make': 'Land Rover',
        'model': '110'},
        'Land Rover;90': {'category': 'luxury', 'make': 'Land Rover', 'model': '90'},
        'Land Rover;Discovery': {'category': 'luxury',
        'make': 'Land Rover',
        'model': 'Discovery'},
        'Land Rover;FREELANDER 2': {'category': 'luxury',
        'make': 'Land Rover',
        'model': 'FREELANDER 2'},
        'Land Rover;Freelander': {'category': 'luxury',
        'make': 'Land Rover',
        'model': 'Freelander'},
        'Land Rover;Range Rover': {'category': 'luxury',
        'make': 'Land Rover',
        'model': 'Range Rover'},
        'Land Rover;Range Rover Evoque': {'category': '4x4',
        'make': 'Land Rover',
        'model': 'Range Rover Evoque'},
        'Land Rover;Range Rover Velar': {'category': 'luxury',
        'make': 'Land Rover',
        'model': 'Range Rover Velar'},
        'Lexus;LS': {'category': 'luxury', 'make': 'Lexus', 'model': 'LS'},
        'Lotus;Elise': {'category': 'luxury', 'make': 'Lotus', 'model': 'Elise'},
        'Lotus;Evora': {'category': 'luxury', 'make': 'Lotus', 'model': 'Evora'},
        'Lotus;Exige': {'category': 'luxury', 'make': 'Lotus', 'model': 'Exige'},
        'MERCEDES-BENZ;AMG': {'category': 'luxury',
        'make': 'MERCEDES-BENZ',
        'model': 'AMG'},
        'MITSUBISHI;L200': {'category': '4x4', 'make': 'MITSUBISHI', 'model': 'L200'},
        'Maserati;Ghibli': {'category': 'luxury',
        'make': 'Maserati',
        'model': 'Ghibli'},
        'Maserati;Levante': {'category': 'luxury',
        'make': 'Maserati',
        'model': 'Levante'},
        'Maserati;Quattroporte': {'category': 'luxury',
        'make': 'Maserati',
        'model': 'Quattroporte'},
        'Mazda;RX-8': {'category': 'luxury', 'make': 'Mazda', 'model': 'RX-8'},
        'Mercedes-Benz;AMG GT': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'AMG GT'},
        'Mercedes-Benz;C-Class': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'C-Class'},
        'Mercedes-Benz;CL': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'CL'},
        'Mercedes-Benz;CLS': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'CLS'},
        'Mercedes-Benz;E-Class': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'E-Class'},
        'Mercedes-Benz;G-Class': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'G-Class'},
        'Mercedes-Benz;GL-Class': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'GL-Class'},
        'Mercedes-Benz;GLA-Class': {'category': '4x4',
        'make': 'Mercedes-Benz',
        'model': 'GLA-Class'},
        'Mercedes-Benz;GLB': {'category': '4x4',
        'make': 'Mercedes-Benz',
        'model': 'GLB'},
        'Mercedes-Benz;GLC-Class': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'GLC-Class'},
        'Mercedes-Benz;GLE': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'GLE'},
        'Mercedes-Benz;M-Class': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'M-Class'},
        'Mercedes-Benz;S-Class': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'S-Class'},
        'Mercedes-Benz;SLS-Class': {'category': 'luxury',
        'make': 'Mercedes-Benz',
        'model': 'SLS-Class'},
        'Mitsubishi;Lancer': {'category': 'luxury',
        'make': 'Mitsubishi',
        'model': 'Lancer'},
        'Mitsubishi;Lancer Evo': {'category': 'luxury',
        'make': 'Mitsubishi',
        'model': 'Lancer Evo'},
        'Morgan;Aero Coupe': {'category': 'luxury',
        'make': 'Morgan',
        'model': 'Aero Coupe'},
        'Morgan;Plus Eight': {'category': 'luxury',
        'make': 'Morgan',
        'model': 'Plus Eight'},
        'NISSAN;NAVARA': {'category': '4x4', 'make': 'NISSAN', 'model': 'NAVARA'},
        'NISSAN;SKYLINE': {'category': 'luxury',
        'make': 'NISSAN',
        'model': 'SKYLINE'},
        'Nissan;X-Trail': {'category': '4x4', 'make': 'Nissan', 'model': 'X-Trail'},
        'Porsche;Cayenne': {'category': 'luxury',
        'make': 'Porsche',
        'model': 'Cayenne'},
        'Porsche;Cayman': {'category': 'luxury',
        'make': 'Porsche',
        'model': 'Cayman'},
        'Porsche;Macan': {'category': 'luxury', 'make': 'Porsche', 'model': 'Macan'},
        'Subaru;Impreza': {'category': 'luxury',
        'make': 'Subaru',
        'model': 'Impreza'},
        'Volkswagen;AMAROK': {'category': '4x4',
        'make': 'Volkswagen',
        'model': 'AMAROK'}}
        self.car_types_dict = car_types_dict

        # margin_cc_dict = {
            
        #     "4x4": {
        #         1:{"margin":994,"min":1000,"max":2001},
        #         2:{"margin":1045,"min":2001,"max":2501},
        #         3:{"margin":1119,"min":2501,"max":3001}
        #     },
        #     "luxury":{
        #         1:{"margin":1046,"min":1000,"max":2001},
        #         2:{"margin":1124,"min":2001,"max":2501},
        #         3:{"margin":1228,"min":2501,"max":3001}
        #     },
        #     "others":{
        #         1:{"margin":949,"min":1000,"max":2001},
        #         2:{"margin":1004,"min":2001,"max":2501},
        #         3:{"margin":1064,"min":2501,"max":3001}
        #     }
        # }

        self.margin_cc_dict = margin_cc_dict

    def get_margin_value(self,make,model,cc,price):
        cc = cc * 1000
        make = str(make).strip()
        model = str(model).strip()
        cc = int(cc)
        car_key = f'{make};{model}'
        
        car_cat = None
        try:
            car_cat = self.car_types_dict[car_key]
        except:
            car_cat = {"category":"others"}
        final_price = 0
        margin = 0
        margin_cc = self.margin_cc_dict[car_cat["category"]]
        for item in margin_cc.items():
            data_item = item[1]
            if cc >= data_item["min"] and cc < data_item["max"]:
                margin = data_item["margin"]
                final_price = price + margin
                break
        if margin == 0:
            status = False
            message = "we are not taking this car due to cc > 3000"
        else:
            status = True
            message = "we are taking this car.all conditions are true."
        
        return {
            "make":make,
            "model":model,
            "cc":cc,
            "car_category":car_cat["category"],
            "input_price":price,
            "new_price":final_price,
            "margin":margin,
            "status":status,
            "message":message
        }

# if __name__ == "__main__":
#     pc = price_conditions()
#     resp = pc.get_margin_value("Pilgrim","Sumo",2.3,19998)
#     print(resp)