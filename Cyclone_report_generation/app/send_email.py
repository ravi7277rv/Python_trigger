from datetime import datetime

import yagmail

from app import config, fetch_data


def get_emails(main_address, district=None):
    # Always get Admin emails
    emails = []
    for entry in main_address:
        if entry["role"].lower() == "admin":
            emails.extend(entry["emails"])

    if district:
        district = district.lower()
        for entry in main_address:
            if entry["role"].lower() == district:
                emails.extend(entry["emails"])

    return emails


def send_with_attachment(path_pdf):
    now = datetime.now()
    formatted_datetime = f"{now.strftime('%d %b %Y')}, {now.strftime('06:00')}"
    yag = yagmail.SMTP(user=config.EMAIL, password=config.PASSWORD)

    # to_mails, cc_mails = fetch_data.get_mail_address()

    html_body = f"""
       Dear All,\n
       Please find attached the Daily Cyclone Report for circles.\n\n
    """
    html_body2 = f"""
       Regards,\n
       ML Infomap | Weather Services
    """

    ml_cc = [
        "ravikumar7277rv@gmail.com",
        "dhiraj@mlinfomap.com",
    ]
    # cc_mails.extend(ml_cc)
    # cc_combined = cc_mails

    yag.send(
        to=[
            "ravikumar7277rv@gmail.com",
            "dhiraj@mlinfomap.com",
        ],
        # to=to_mails,
        cc=[
            "ravikumar7277rv@gmail.com",
            "dhiraj@mlinfomap.com",
        ],
        subject=f"Daily Cyclone Report for Circles - {formatted_datetime} Hrs",
        contents=[html_body, yagmail.inline(path_pdf), html_body2],
        # contents=[html_body],
        attachments=[path_pdf],
    )
    print("✅ Email sent successfully!")
