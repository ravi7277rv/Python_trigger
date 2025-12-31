from app import config
import requests
from datetime import datetime
from app import fetch_data
from urllib.parse import quote

def get_phone_numbers(phone_numbers, district=None):
    numbers = []
    for entry in phone_numbers:
        if entry["role"].lower() == "admin":
            numbers.extend(entry["numbers"])
    
    if district:
        district = district.lower()
        for entry in phone_numbers:
            if entry["role"].lower() == district:
                numbers.extend(entry["numbers"])

    return numbers

def get_current_time_ampm(pdf_url):
    # return datetime.now().strftime("%#I:%M %p")
    now = datetime.now()
    return f"{now.strftime('%d %b %Y')} {now.strftime('06:00')} Hrs. Report Link - {pdf_url}"

def send_whatsapp_message(circle):

    today_str = datetime.now().strftime("%d %b %Y")   # 07 Nov 2025
    file_name = f"{circle} circle - {today_str}"

    image_url = f"https://mlinfomap.com/weather/reports/one_pager_circle/{file_name}.png"
    pdf_url = f"https://mlinfomap.com/weather/reports/pdf_circle/{quote(file_name)}.pdf"
    # image_url = f"https://mlinfomap.com/weather/Varanasi_report.png"

    phone_numbers = fetch_data.get_mobile_numbers(circle)
    # phone_numbers = [{"name":"Er. Rizwan","mobile":"919315132167"}]

    date_time = get_current_time_ampm(pdf_url)

    integrated_number = "919818462244"
    template_name = "weather_report_info"
    namespace = "1aa09f9b_cb85_4abc_ac96_8db7ddb96bb5"
    url = "https://api.msg91.com/api/v5/whatsapp/whatsapp-outbound-message/bulk/"
    headers = {
        "Content-Type": "application/json",
        "authkey": config.whatsapp_authkey,
        "Cookie": "HELLO_APP_HASH=OVZPOEVxUGR6ZktIUmdyQzFpbXlZeHVHSUNoRCtUSWRuTElZV3lkYmVNWT0%3D; PHPSESSID=v02kr4vb1o069bak5rcn4kccod"
    }

    # payload = {
    #     "integrated_number": integrated_number,
    #     "content_type": "template",
    #     "payload": {
    #         "messaging_product": "whatsapp",
    #         "type": "template",
    #         "template": {
    #             "name": template_name,
    #             "language": {
    #                 "code": "en",
    #                 "policy": "deterministic"
    #             },
    #             "namespace": namespace,
    #             "to_and_components": [
    #                 {
    #                     "to": phone_numbers,
    #                     "components": {
    #                         "header_1": {
    #                             "type": "image",
    #                             "value": image_url
    #                         }
    #                     }
    #                 }
    #             ]
    #         }
    #     }
    # }

    # print(phone_numbers)

    if len(phone_numbers) > 0:
        for record in phone_numbers:
            payload = {
                "integrated_number": integrated_number,
                "content_type": "template",
                "payload": {
                    "messaging_product": "whatsapp",
                    "type": "template",
                    "template": {
                        "name": template_name,
                        "language": {
                            "code": "en",
                            "policy": "deterministic"
                        },
                        "namespace": namespace,
                        "to_and_components": [
                            {
                                "to": f"91{record["mobile"]}",
                                # "to": "91",
                                "components": {
                                    "header_1": {
                                        "type": "image",
                                        "value": image_url
                                    },
                                    "body_1": {
                                        "type": "text",
                                        "value": record["name"]
                                    },
                                    "body_2": {
                                        "type": "text",
                                        "value": date_time
                                    }
                                }
                            }
                        ]
                    }
                }
            }

            # print(payload)
            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 200:
                print("✅ WhatsApp message sent successfully")
                # return response.json()
            else:
                print(f"❌ Failed to send message: {response.status_code}")
                print(response.text)
                # return None
