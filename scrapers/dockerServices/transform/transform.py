import pulsar
import json


class transform:
    def __init__(self):
        print(f'transform init')
        
        self.topicTransform = 'motormarket.scrapers.autotrader.listing.transform'
        
        self.topicValidate = 'motormarket.scrapers.autotrader.listing.predict.makemodel'
        
        self.uri = 'pulsar://pulsar'
        
        self.client = pulsar.Client(self.uri)
        
        self.producer = self.client.create_producer(self.topicValidate)
        
        self.consumer = self.client.subscribe(self.topicTransform, 'Transform-subscription')
        
        self.transmissionCode = {
            "automatic":1,
            "manual":2,
            "automanual":3,
            "auto":1,
            "auto clutch":1,
            "auto/manual mode":1,
            "cvt":1,
            "cvt/manual mode":1,
            "manual transmission":2,
            "semi-automatic":1,
            "sequential":1
        }
        
        self.fuelCode = {
            "petrol":1,
            "diesel":2,
            "gas":3,
            "hybrid":5,
            "electric":6,
            "hybrid – petrol/elec":7,
            "hybrid –":8,
        }
        
        self.builtCode = {
            "64":2014,
            "14":2014,
            "65":2015,
            "15":2015,
            "66":2016,
            "16":2016,
            "67":2017,
            "17":2017,
            "68":2018,
            "18":2018,
            "69":2019,
            "19":2019,
            "70":2020,
            "20":2020,
            "71":2021,
            "21":2021
        }
        
        self.bodyStyles = [
                           {"from":"coupe","to":"Coupe"},
                           {"from":"stationwagon","to":"Estate"},
                           {"from":"van","to":"Van"},
                           {"from":"lvc","to":"Van"},
                           {"from":"standard roof minibus","to":"Minibus"},
                           {"from":"estate","to":"Estate"},
                           {"from":"convertible","to":"Convertible"},
                           {"from":"mpv","to":"MPV"},
                           {"from":"sedan","to":"Saloon"},
                           {"from":"hatchback","to":"Hatchback"},
                           {"from":"saloon","to":"Saloon"},
                           {"from":"suv","to":"SUV"},
                           {"from":"combi van","to":"Van"},
                           {"from":"wheelchair adapted vehicle - w","to":"Wheelchair"},
                           {"from":"double cab pick-up","to":"Pickup"}
        ]
        
        self.make4x4 = [
            "cadillac",
            "jeep"
        ]
        
        self.makeLuxury = [
            "caterham", 
            "hummer", 
            "infiniti", 
            "jaguar", 
            "morgan",
            "lotus"
        ]
        
        self.marginBasedOnCC = {

            "4x4": {
                0: {"margin": 1370, "min": 0, "max": 1001},
                1: {"margin": 1370, "min": 1001, "max": 2001},
                2: {"margin": 1420, "min": 2001, "max": 2501},
                3: {"margin": 1545, "min": 2501, "max": 3001}
            },
            "luxury": {
                0: {"margin": 1425, "min": 0, "max": 1001},
                1: {"margin": 1425, "min": 1001, "max": 2001},
                2: {"margin": 1499, "min": 2001, "max": 2501},
                3: {"margin": 1599, "min": 2501, "max": 3001}
            },
            "others": {
                0: {"margin": 1299, "min": 0, "max": 1001},
                1: {"margin": 1299, "min": 1001, "max": 2001},
                2: {"margin": 1355, "min": 2001, "max": 2501},
                3: {"margin": 1415, "min": 2501, "max": 3001}
            }
        }
        
        self.carTypesDict = {'alfa romeo_stelvio quadrifoglio': {'make': 'alfa romeo',
                                                                   'model': 'stelvio quadrifoglio',
                                                                   'category': '4x4'},
                               'alfa romeo_stelvio': {'make': 'alfa romeo',
                                                      'model': 'stelvio',
                                                      'category': '4x4'},
                               'audi_q2': {'make': 'audi', 'model': 'q2', 'category': '4x4'},
                               'audi_q3': {'make': 'audi', 'model': 'q3', 'category': '4x4'},
                               'bmw_x1': {'make': 'bmw', 'model': 'x1', 'category': '4x4'},
                               'bmw_x2': {'make': 'bmw', 'model': 'x2', 'category': '4x4'},
                               'bmw_x3': {'make': 'bmw', 'model': 'x3', 'category': '4x4'},
                               'cadillac_bls': {'make': 'cadillac', 'model': 'bls', 'category': '4x4'},
                               'cadillac_escalade': {'make': 'cadillac',
                                                     'model': 'escalade',
                                                     'category': '4x4'},
                               'chevrolet_captiva': {'make': 'chevrolet',
                                                     'model': 'captiva',
                                                     'category': '4x4'},
                               'dodge_avenger': {'make': 'dodge', 'model': 'avenger', 'category': '4x4'},
                               'dodge_caliber': {'make': 'dodge', 'model': 'caliber', 'category': '4x4'},
                               'dodge_charger': {'make': 'dodge', 'model': 'charger', 'category': '4x4'},
                               'dodge_nitro': {'make': 'dodge', 'model': 'nitro', 'category': '4x4'},
                               'dodge_ram': {'make': 'dodge', 'model': 'ram', 'category': '4x4'},
                               'ford_edge': {'make': 'ford', 'model': 'edge', 'category': '4x4'},
                               'ford_ranger pickup': {'make': 'ford',
                                                      'model': 'ranger pickup',
                                                      'category': '4x4'},
                               'jaguar_e-pace': {'make': 'jaguar', 'model': 'e-pace', 'category': 'luxury'},
                               'jaguar_f-pace': {'make': 'jaguar', 'model': 'f-pace', 'category': 'luxury'},
                               'jaguar_i-pace': {'make': 'jaguar', 'model': 'i-pace', 'category': 'luxury'},
                               'jeep_cherokee': {'make': 'jeep', 'model': 'cherokee', 'category': '4x4'},
                               'jeep_commander': {'make': 'jeep', 'model': 'commander', 'category': '4x4'},
                               'jeep_compass': {'make': 'jeep', 'model': 'compass', 'category': '4x4'},
                               'jeep_grand cherokee': {'make': 'jeep',
                                                       'model': 'grand cherokee',
                                                       'category': '4x4'},
                               'jeep_patriot': {'make': 'jeep', 'model': 'patriot', 'category': '4x4'},
                               'jeep_renegade': {'make': 'jeep', 'model': 'renegade', 'category': '4x4'},
                               'jeep_wrangler': {'make': 'jeep', 'model': 'wrangler', 'category': '4x4'},
                               'land rover_range rover evoque': {'make': 'land rover',
                                                                 'model': 'range rover evoque',
                                                                 'category': '4x4'},
                               'mercedes_gla': {'make': 'mercedes', 'model': 'gla', 'category': '4x4'},
                               'mercedes_glb': {'make': 'mercedes', 'model': 'glb', 'category': '4x4'},
                               'mercedes_glc': {'make': 'mercedes', 'model': 'glc', 'category': '4x4'},
                               'mitsubishi_l200 pickup': {'make': 'mitsubishi',
                                                          'model': 'l200 pickup',
                                                          'category': '4x4'},
                               'nissan_navara pickup': {'make': 'nissan',
                                                        'model': 'navara pickup',
                                                        'category': '4x4'},
                               'nissan_x-trail': {'make': 'nissan', 'model': 'x-trail', 'category': '4x4'},
                               'volkswagen_amarok': {'make': 'volkswagen',
                                                     'model': 'amarok',
                                                     'category': '4x4'},
                               'alfa romeo_4c': {'make': 'alfa romeo', 'model': '4c', 'category': 'luxury'},
                               'alfa romeo_8c': {'make': 'alfa romeo', 'model': '8c', 'category': 'luxury'},
                               'audi_q5': {'make': 'audi', 'model': 'q5', 'category': 'luxury'},
                               'audi_q7': {'make': 'audi', 'model': 'q7', 'category': 'luxury'},
                               'audi_q8': {'make': 'audi', 'model': 'q8', 'category': 'luxury'},
                               'audi_rs q3': {'make': 'audi', 'model': 'rs q3', 'category': 'luxury'},
                               'audi_rs3': {'make': 'audi', 'model': 'rs3', 'category': 'luxury'},
                               'audi_rs4': {'make': 'audi', 'model': 'rs4', 'category': 'luxury'},
                               'audi_rs5': {'make': 'audi', 'model': 'rs5', 'category': 'luxury'},
                               'audi_rs6': {'make': 'audi', 'model': 'rs6', 'category': 'luxury'},
                               'audi_rs7': {'make': 'audi', 'model': 'rs7', 'category': 'luxury'},
                               'audi_s1 quattro': {'make': 'audi',
                                                   'model': 's1 quattro',
                                                   'category': 'luxury'},
                               'audi_s3 quattro': {'make': 'audi',
                                                   'model': 's3 quattro',
                                                   'category': 'luxury'},
                               'audi_s4 quattro': {'make': 'audi',
                                                   'model': 's4 quattro',
                                                   'category': 'luxury'},
                               'audi_s4': {'make': 'audi', 'model': 's4', 'category': 'luxury'},
                               'audi_s5 quattro': {'make': 'audi',
                                                   'model': 's5 quattro',
                                                   'category': 'luxury'},
                               'audi_s5': {'make': 'audi', 'model': 's5', 'category': 'luxury'},
                               'audi_s6 quattro': {'make': 'audi',
                                                   'model': 's6 quattro',
                                                   'category': 'luxury'},
                               'audi_s6': {'make': 'audi', 'model': 's6', 'category': 'luxury'},
                               'audi_s7 quattro': {'make': 'audi',
                                                   'model': 's7 quattro',
                                                   'category': 'luxury'},
                               'audi_s8 quattro': {'make': 'audi',
                                                   'model': 's8 quattro',
                                                   'category': 'luxury'},
                               'audi_sq': {'make': 'audi', 'model': 'sq', 'category': 'luxury'},
                               'audi_sq5': {'make': 'audi', 'model': 'sq5', 'category': 'luxury'},
                               'bmw_alpina': {'make': 'bmw', 'model': 'alpina', 'category': 'luxury'},
                               'bmw_i3': {'make': 'bmw', 'model': 'i3', 'category': 'luxury'},
                               'bmw_m1': {'make': 'bmw', 'model': 'm1', 'category': 'luxury'},
                               'bmw_m135i': {'make': 'bmw', 'model': 'm135i', 'category': 'luxury'},
                               'bmw_m140i': {'make': 'bmw', 'model': 'm140i', 'category': 'luxury'},
                               'bmw_m2 competition': {'make': 'bmw',
                                                      'model': 'm2 competition',
                                                      'category': 'luxury'},
                               'bmw_m2': {'make': 'bmw', 'model': 'm2', 'category': 'luxury'},
                               'bmw_m235i': {'make': 'bmw', 'model': 'm235i', 'category': 'luxury'},
                               'bmw_m3 competition': {'make': 'bmw',
                                                      'model': 'm3 competition',
                                                      'category': 'luxury'},
                               'bmw_m3': {'make': 'bmw', 'model': 'm3', 'category': 'luxury'},
                               'bmw_m4 competition': {'make': 'bmw',
                                                      'model': 'm4 competition',
                                                      'category': 'luxury'},
                               'bmw_m4': {'make': 'bmw', 'model': 'm4', 'category': 'luxury'},
                               'bmw_m5 competition': {'make': 'bmw',
                                                      'model': 'm5 competition',
                                                      'category': 'luxury'},
                               'bmw_m5': {'make': 'bmw', 'model': 'm5', 'category': 'luxury'},
                               'bmw_m6 competition': {'make': 'bmw',
                                                      'model': 'm6 competition',
                                                      'category': 'luxury'},
                               'bmw_m6': {'make': 'bmw', 'model': 'm6', 'category': 'luxury'},
                               'bmw_x4 m': {'make': 'bmw', 'model': 'x4 m', 'category': 'luxury'},
                               'bmw_x4': {'make': 'bmw', 'model': 'x4', 'category': 'luxury'},
                               'bmw_x5 m': {'make': 'bmw', 'model': 'x5 m', 'category': 'luxury'},
                               'bmw_x5': {'make': 'bmw', 'model': 'x5', 'category': 'luxury'},
                               'bmw_x6 m': {'make': 'bmw', 'model': 'x6 m', 'category': 'luxury'},
                               'bmw_x6': {'make': 'bmw', 'model': 'x6', 'category': 'luxury'},
                               'bmw_x7': {'make': 'bmw', 'model': 'x7', 'category': 'luxury'},
                               'bmw_z4': {'make': 'bmw', 'model': 'z4', 'category': 'luxury'},
                               'caterham_7 supersport': {'make': 'caterham',
                                                         'model': '7 supersport',
                                                         'category': 'luxury'},
                               'caterham_7.0': {'make': 'caterham', 'model': '7.0', 'category': 'luxury'},
                               'chevrolet_corvette': {'make': 'chevrolet',
                                                      'model': 'corvette',
                                                      'category': 'luxury'},
                               'dodge_challenger': {'make': 'dodge',
                                                    'model': 'challenger',
                                                    'category': 'luxury'},
                               'ford_mustang': {'make': 'ford', 'model': 'mustang', 'category': 'luxury'},
                               'ford_rs': {'make': 'ford', 'model': 'rs', 'category': 'luxury'},
                               'ford_focus st': {'make': 'ford', 'model': 'focus st', 'category': 'luxury'},
                               'hummer_h2': {'make': 'hummer', 'model': 'h2', 'category': 'luxury'},
                               'hummer_h3': {'make': 'hummer', 'model': 'h3', 'category': 'luxury'},
                               'infiniti_q30': {'make': 'infiniti', 'model': 'q30', 'category': 'luxury'},
                               'infiniti_q50': {'make': 'infiniti', 'model': 'q50', 'category': 'luxury'},
                               'infiniti_q60': {'make': 'infiniti', 'model': 'q60', 'category': 'luxury'},
                               'infiniti_q70': {'make': 'infiniti', 'model': 'q70', 'category': 'luxury'},
                               'infiniti_qx50': {'make': 'infiniti', 'model': 'qx50', 'category': 'luxury'},
                               'infiniti_qx55': {'make': 'infiniti', 'model': 'qx55', 'category': 'luxury'},
                               'infiniti_qx60': {'make': 'infiniti', 'model': 'qx60', 'category': 'luxury'},
                               'infiniti_qx70': {'make': 'infiniti', 'model': 'qx70', 'category': 'luxury'},
                               'jaguar_s-type': {'make': 'jaguar', 'model': 's-type', 'category': 'luxury'},
                               'jaguar_x-type': {'make': 'jaguar', 'model': 'x-type', 'category': 'luxury'},
                               'jaguar_xe': {'make': 'jaguar', 'model': 'xe', 'category': 'luxury'},
                               'jaguar_xf': {'make': 'jaguar', 'model': 'xf', 'category': 'luxury'},
                               'jaguar_xfr': {'make': 'jaguar', 'model': 'xfr', 'category': 'luxury'},
                               'jaguar_xj': {'make': 'jaguar', 'model': 'xj', 'category': 'luxury'},
                               'jaguar_xjl': {'make': 'jaguar', 'model': 'xjl', 'category': 'luxury'},
                               'jaguar_xjr': {'make': 'jaguar', 'model': 'xjr', 'category': 'luxury'},
                               'jaguar_xk': {'make': 'jaguar', 'model': 'xk', 'category': 'luxury'},
                               'jaguar_xk8': {'make': 'jaguar', 'model': 'xk8', 'category': 'luxury'},
                               'jaguar_xkr': {'make': 'jaguar', 'model': 'xkr', 'category': 'luxury'},
                               'land rover_autobiography': {'make': 'land rover',
                                                            'model': 'autobiography',
                                                            'category': 'luxury'},
                               'land rover_defender 110': {'make': 'land rover',
                                                           'model': 'defender 110',
                                                           'category': 'luxury'},
                               'land rover_defender 130': {'make': 'land rover',
                                                           'model': 'defender 130',
                                                           'category': 'luxury'},
                               'land rover_defender 90': {'make': 'land rover',
                                                          'model': 'defender 90',
                                                          'category': 'luxury'},
                               'land rover_discovery 3': {'make': 'land rover',
                                                          'model': 'discovery 3',
                                                          'category': 'luxury'},
                               'land rover_discovery 4': {'make': 'land rover',
                                                          'model': 'discovery 4',
                                                          'category': 'luxury'},
                               'land rover_discovery sport': {'make': 'land rover',
                                                              'model': 'discovery sport',
                                                              'category': 'luxury'},
                               'land rover_freelander 1': {'make': 'land rover',
                                                           'model': 'freelander 1',
                                                           'category': 'luxury'},
                               'land rover_freelander 2': {'make': 'land rover',
                                                           'model': 'freelander 2',
                                                           'category': 'luxury'},
                               'land rover_range rover sport': {'make': 'land rover',
                                                                'model': 'range rover sport',
                                                                'category': 'luxury'},
                               'land rover_range rover velar': {'make': 'land rover',
                                                                'model': 'range rover velar',
                                                                'category': 'luxury'},
                               'land rover_range rover vogue': {'make': 'land rover',
                                                                'model': 'range rover vogue',
                                                                'category': 'luxury'},
                               'land rover_range rover': {'make': 'land rover',
                                                          'model': 'range rover',
                                                          'category': 'luxury'},
                               'lexus_ls': {'make': 'lexus', 'model': 'ls', 'category': 'luxury'},
                               'lotus_elise': {'make': 'lotus', 'model': 'elise', 'category': 'luxury'},
                               'lotus_evora': {'make': 'lotus', 'model': 'evora', 'category': 'luxury'},
                               'lotus_exige': {'make': 'lotus', 'model': 'exige', 'category': 'luxury'},
                               'maserati_ghibli': {'make': 'maserati',
                                                   'model': 'ghibli',
                                                   'category': 'luxury'},
                               'maserati_levante': {'make': 'maserati',
                                                    'model': 'levante',
                                                    'category': 'luxury'},
                               'maserati_quattroporte': {'make': 'maserati',
                                                         'model': 'quattroporte',
                                                         'category': 'luxury'},
                               'mazda_rx-8': {'make': 'mazda', 'model': 'rx-8', 'category': 'luxury'},
                               'mercedes_amg gt s': {'make': 'mercedes',
                                                     'model': 'amg gt s',
                                                     'category': 'luxury'},
                               'mercedes_amg gt': {'make': 'mercedes',
                                                   'model': 'amg gt',
                                                   'category': 'luxury'},
                               'mercedes_c43 amg': {'make': 'mercedes',
                                                    'model': 'c43 amg',
                                                    'category': 'luxury'},
                               'mercedes_c63 amg': {'make': 'mercedes',
                                                    'model': 'c63 amg',
                                                    'category': 'luxury'},
                               'mercedes_cl63 amg': {'make': 'mercedes',
                                                     'model': 'cl63 amg',
                                                     'category': 'luxury'},
                               'mercedes_cl65 amg': {'make': 'mercedes',
                                                     'model': 'cl65 amg',
                                                     'category': 'luxury'},
                               'mercedes_cla35 amg': {'make': 'mercedes',
                                                      'model': 'cla35 amg',
                                                      'category': 'luxury'},
                               'mercedes_cla45 amg': {'make': 'mercedes',
                                                      'model': 'cla45 amg',
                                                      'category': 'luxury'},
                               'mercedes_cls 63 amg s': {'make': 'mercedes',
                                                         'model': 'cls 63 amg s',
                                                         'category': 'luxury'},
                               'mercedes_cls53 amg': {'make': 'mercedes',
                                                      'model': 'cls53 amg',
                                                      'category': 'luxury'},
                               'mercedes_cls63 amg s': {'make': 'mercedes',
                                                        'model': 'cls63 amg s',
                                                        'category': 'luxury'},
                               'mercedes_cls63 amg': {'make': 'mercedes',
                                                      'model': 'cls63 amg',
                                                      'category': 'luxury'},
                               'mercedes_e43 amg': {'make': 'mercedes',
                                                    'model': 'e43 amg',
                                                    'category': 'luxury'},
                               'mercedes_e53 amg': {'make': 'mercedes',
                                                    'model': 'e53 amg',
                                                    'category': 'luxury'},
                               'mercedes_e63 amg': {'make': 'mercedes',
                                                    'model': 'e63 amg',
                                                    'category': 'luxury'},
                               'mercedes_g': {'make': 'mercedes', 'model': 'g', 'category': 'luxury'},
                               'mercedes_g350 bluetec': {'make': 'mercedes',
                                                         'model': 'g350 bluetec',
                                                         'category': 'luxury'},
                               'mercedes_g55 amg': {'make': 'mercedes',
                                                    'model': 'g55 amg',
                                                    'category': 'luxury'},
                               'mercedes_g63 amg': {'make': 'mercedes',
                                                    'model': 'g63 amg',
                                                    'category': 'luxury'},
                               'mercedes_gl': {'make': 'mercedes', 'model': 'gl', 'category': 'luxury'},
                               'mercedes_gl63 amg': {'make': 'mercedes',
                                                     'model': 'gl63 amg',
                                                     'category': 'luxury'},
                               'mercedes_gla45 amg': {'make': 'mercedes',
                                                      'model': 'gla45 amg',
                                                      'category': 'luxury'},
                               'mercedes_glc43 amg': {'make': 'mercedes',
                                                      'model': 'glc43 amg',
                                                      'category': 'luxury'},
                               'mercedes_glc63 amg': {'make': 'mercedes',
                                                      'model': 'glc63 amg',
                                                      'category': 'luxury'},
                               'mercedes_gle': {'make': 'mercedes', 'model': 'gle', 'category': 'luxury'},
                               'mercedes_gle43 amg': {'make': 'mercedes',
                                                      'model': 'gle43 amg',
                                                      'category': 'luxury'},
                               'mercedes_gle63 amg': {'make': 'mercedes',
                                                      'model': 'gle63 amg',
                                                      'category': 'luxury'},
                               'mercedes_gls': {'make': 'mercedes', 'model': 'gls', 'category': 'luxury'},
                               'mercedes_gls63 amg': {'make': 'mercedes',
                                                      'model': 'gls63 amg',
                                                      'category': 'luxury'},
                               'mercedes_ml': {'make': 'mercedes', 'model': 'ml', 'category': 'luxury'},
                               'mercedes_ml63 amg': {'make': 'mercedes',
                                                     'model': 'ml63 amg',
                                                     'category': 'luxury'},
                               'mercedes_s63 amg': {'make': 'mercedes',
                                                    'model': 's63 amg',
                                                    'category': 'luxury'},
                               'mercedes_s65 amg': {'make': 'mercedes',
                                                    'model': 's65 amg',
                                                    'category': 'luxury'},
                               'mercedes_sl55 amg': {'make': 'mercedes',
                                                     'model': 'sl55 amg',
                                                     'category': 'luxury'},
                               'mercedes_sl63 amg': {'make': 'mercedes',
                                                     'model': 'sl63 amg',
                                                     'category': 'luxury'},
                               'mercedes_sl65 amg': {'make': 'mercedes',
                                                     'model': 'sl65 amg',
                                                     'category': 'luxury'},
                               'mercedes_slc43 amg': {'make': 'mercedes',
                                                      'model': 'slc43 amg',
                                                      'category': 'luxury'},
                               'mercedes_slk55 amg': {'make': 'mercedes',
                                                      'model': 'slk55 amg',
                                                      'category': 'luxury'},
                               'mercedes_sls amg': {'make': 'mercedes',
                                                    'model': 'sls amg',
                                                    'category': 'luxury'},
                               'mitsubishi_lancer evo': {'make': 'mitsubishi',
                                                         'model': 'lancer evo',
                                                         'category': 'luxury'},
                               'mitsubishi_lancer ralliart': {'make': 'mitsubishi',
                                                              'model': 'lancer ralliart',
                                                              'category': 'luxury'},
                               'mitsubishi_lancer': {'make': 'mitsubishi',
                                                     'model': 'lancer',
                                                     'category': 'luxury'},
                               'morgan_aero': {'make': 'morgan', 'model': 'aero', 'category': 'luxury'},
                               'morgan_plus 8': {'make': 'morgan', 'model': 'plus 8', 'category': 'luxury'},
                               'nissan_skyline': {'make': 'nissan',
                                                  'model': 'skyline',
                                                  'category': 'luxury'},
                               'porsche_cayenne': {'make': 'porsche',
                                                   'model': 'cayenne',
                                                   'category': 'luxury'},
                               'porsche_cayman': {'make': 'porsche',
                                                  'model': 'cayman',
                                                  'category': 'luxury'},
                               'porsche_macan': {'make': 'porsche', 'model': 'macan', 'category': 'luxury'},
                               'subaru_impreza turbo': {'make': 'subaru',
                                                        'model': 'impreza turbo',
                                                        'category': 'luxury'},
                               'subaru_impreza wrx': {'make': 'subaru',
                                                      'model': 'impreza wrx',
                                                      'category': 'luxury'},
                               'subaru_impreza': {'make': 'subaru',
                                                  'model': 'impreza',
                                                  'category': 'luxury'}}

    
    def calculateMargin(self,make,model,cc):
        
        if make == None or model == None or cc == None:
            return None
        
        key = f'{make}_{model}'
        
        if make in self.make4x4:
            category = "4x4"
        elif make in self.makeLuxury:
            category = "luxury"
        else:
            category = self.carTypesDict.get(key,None)
            
            if category == None:
                category = "others"
        
        margin = None
        
        marginDict = self.marginBasedOnCC[category]
        
        for key in marginDict:
            data = marginDict[key]
            
            if cc >= data["min"] and cc < data["max"]:
                margin = data["margin"]
                break
        
        return margin
            
        
        

    def consume(self):
        
        while True:
            message = self.consumer.receive()
            
            try:
                data = json.loads(message.data)
                
                self.consumer.acknowledge(message)
                
                transformedData = self.transformData(data["data"])
                
                self.produce(transformedData)
                
            except Exception as e:
                print(f'error : {str(e)}')
    
    def produce(self,data):
        
        self.producer.send(
            json.dumps(data).encode("utf-8")
        )
        
    def __del__(self):
        self.client.close()
    
    def transformData(self,data):
        
        # dealer Name
        if data["dealerName"] != None:
            dealerName = data["dealerName"].strip().lower()
            data["dealerName"] = dealerName
            
        # dealer Number
        if data["dealerNumber"] != None:
            dealerNumber = data["dealerNumber"].strip().lower()
            data["dealerNumber"] = dealerNumber
            
        # dealer Location
        if data["dealerLocation"] != None:
            dealerLocation = data["dealerLocation"].strip().replace(" ","").upper()
            data["dealerLocation"] = dealerLocation
            
        #  location
        if data["location"] != None:
            location = data["location"].strip().replace(" ","").upper()
            data["location"] = location
            
        # dealer Id
        if data["dealerId"] != None:
            dealerId = data["dealerId"].strip()
            data["dealerId"] = dealerId
        
        #  wheelBase
        
        # cabtype
        if data["cabType"] != None:
            cabType = str(data["cabType"]).lower().strip()
            data["cabType"] = cabType
        
        # make
        if data["make"] != None:
            make = str(data["make"]).lower().strip()
            data["make"] = make
        
        # model
        if data["model"] != None:
            model = str(data["model"]).lower().strip()
            data["model"] = model
        
        # engineCylinders
        if data["engineCylinders"] != None:
            engineCylindersCC = int(data["engineCylinders"])
            data["engineCylindersCC"] = engineCylindersCC
            
            engineCylindersLitre = engineCylindersCC/1000
            data["engineCylindersLitre"] = round(engineCylindersLitre,2)
        
        # registration
        if data["registration"] != None:
            registration = str(data["registration"]).strip().upper()
            data["registration"] = registration
        
        # built
        if data["built"] != None:
            built = int(data["built"])
            data["built"] = built
        else:
            code = [str(char) for char in data["registration"] if char.isdigit()]
            code = "".join(code)
            
            if code in self.builtCode:
                built = self.builtCode[code]
                data["built"] = built
        
        # seats
        if data["seats"] != None:
            seats = int(data["seats"])
            data["seats"] = seats
        
        # mileage
        if data["mileage"] != None:
            mileage = int(data["mileage"])
            data["mileage"] = mileage
        
        # fuel
        if data["fuel"] != None:
            fuel = str(data["fuel"]).strip().lower()
            data["fuel"] = fuel
        
        # writeOffCategory
        if data["writeOffCategory"] != None:
            writeOffCategory = str(data["writeOffCategory"]).strip().lower()
            data["writeOffCategory"] = writeOffCategory
            
        # doors
        if data["doors"] != None:
            doors = int(data["doors"])
            data["doors"] = doors
        
        # bodyStyle
        if data["bodyStyle"] != None:
            bodyStyle = str(data["bodyStyle"]).strip().lower()
            data["bodyStyle"] = bodyStyle
            data["bodyStylePredicted"] = None
            
            for bs in self.bodyStyles:
                if bodyStyle in bs["from"].lower():
                    data["bodyStylePredicted"] = bs["to"]
                    break
        
        # price
        if data["price"] != None:
            price = int(data["price"])
            data["price"] = price
        
        # priceIndicator
        if data["priceIndicator"] != None:
            priceIndicator = str(data["priceIndicator"]).strip().lower()
            data["priceIndicator"] = priceIndicator
        
        # adminFee
        if data["adminFee"] != None:
            adminFee = int(data["adminFee"])
            data["adminFee"] = adminFee
        
        # trim
        if data["trim"] != None:
            trim = str(data["trim"]).strip().lower()
            data["trim"] = trim
        
        # vehicleType
        if data["vehicleType"] != None:
            vehicleType = str(data["vehicleType"]).strip().lower()
            data["vehicleType"] = vehicleType
        
        # emissionScheme
        
        # transmission
        if data["transmission"] != None:
            transmission = str(data["transmission"]).strip().lower()
            data["transmission"] = transmission
        
        # id
        if data["id"] != None:
            id = str(data["id"]).strip()
            data["id"] = id
        
        # images
        images = []
        for img in data["images"]:
            images.append(img["url"])
        
        data["images"] = images
        
        # url
        
        # add / calc new fields
        # title
        
        data["title"] = f'{data["make"]} {data["model"]} {data["trim"]}'.replace("None","").title()
        
        # transmissionCode
        if data["transmission"] in self.transmissionCode:
            data["transmissionCode"] = self.transmissionCode[data["transmission"]]
        else:
            data["transmissionCode"] = 4
        
        # fuelCode
        if data["fuel"] in self.fuelCode:
            data["fuelCode"] = self.fuelCode[data["fuel"]]
        else:
            data["fuelCode"] = 4
        
        # margin
        margin = self.calculateMargin(data["make"],data["model"],data["engineCylindersCC"])
        
        data["margin"] = margin    
        
        return data