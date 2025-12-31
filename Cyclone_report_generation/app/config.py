# app/config.py

EMAIL = "post@mlinfomap.com"
PASSWORD = "tmisxyakmbllotlw"

# whatsapp_authkey = "465024Abta9Wp0NQ2K68a31f3cP1"

# phone_numbers = ["919639548485"]

phone_numbers = [
    {
        "role": "Admin",
        "numbers": [
            # {"name":"Er. Rizwan","mobile":"919315132167"},
            # {"name":"Ravi Kumar","mobile":"918083289760"},
            # {"name":"Subodh Kumar","mobile":"919560604422"},
            # {"name":"Atul Kapoor","mobile":"919810294592"},
            # {"name":"Aditya Sareen","mobile":"919818338464"},
            # {"name":"Yogendra Singh","mobile":"919654785670"}, # ML Office
            # {"name":"Jatin Gautam","mobile":"919910995024"},
            # {"name":"Arun Sharma","mobile":"919871320275"},
            # {"name":"M Siva Kumar","mobile":"919962001764"} # Client
        ],
    },
    # {
    #     "role":'Varanasi',
    #     "numbers":[
    #         {"name":"Shailesh Singh","mobile":"919794652453"},
    #         {"name":"Ashish Gupta","mobile":"918535006973"}
    #         ]
    # },
    # {
    #     "role":'Gwalior',
    #     "numbers" : [
    #         {"name":"Abhishek Shrivastava","mobile":"919630090570"}
    #         ]
    # }
]

# CC_RECEIVERS = ['rizwansiddiqui5225@gmail.com','atul@mlinfomap.com']
# CC_RECEIVERS = ['subodh@mlinfomap.com','atul@mlinfomap.com','Aditya@mlinfomap.com','rizwansiddiqui5225@gmail.com']

# main_address = [
#     {
#         "role":"Admin",
#         "emails":["subodh@mlinfomap.com",]
#     },
#     {
#         "role":'Varanasi',
#         "emails":["aditya@mlinfomap.com"]
#     },
#     {
#         "role":'Gwalior',
#         "emails" : ["rizwansiddiqui5225@gmail.com"]
#     }
#     ]

# main_address = [
#     {
#         "role":"Admin",
#         "emails":["msiva.kumar@industowers.com",
#                 "rakesh.gupta@industowers.com",
#                 "prakhar.verma@industowers.com",
#                 "srinath.kalimuthu@industowers.com",
#                 "ashish.gupta3@industowers.com",
#                 "arun.sharma@industowers.com",
#                 "jatin.gautam@industowers.com"]
#     },
#     {
#         "role":'Varanasi',
#         "emails":["mukeshkumar.choubey@industowers.com",
#             "dharmendra.kumar@industowers.com",
#             "shailesh.singh@industowers.com"]
#     },
#     {
#         "role":'Gwalior',
#         "emails" : ["alekh.saini@industowers.com",
#                    "abhishek.s2@industowers.com"]
#     }
#     ]

# Database credential

# DB_HOST = '3.108.39.84'
DB_HOST = "mlinfomap.org"
DB = "cris"
DB_USER = "postgres"
DB_PASSWORD = "Intel%401968"
DB_PORT = 5432

# Weather API
API_KEY = "ce3f4317d6204d0f99571656250108"
API_URL = "https://api.weatherapi.com/v1/forecast.json"
