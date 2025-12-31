import glob
import os
import shutil
import time
import webbrowser
from datetime import datetime

from app import export_pdf, fetch_data, render_html, send_email


def clean_output_folder():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "output")
    patterns = ["*.pdf", "*.png"]

    deleted_files = []
    for pattern in patterns:
        files = glob.glob(os.path.join(output_dir, pattern))
        for f in files:
            try:
                os.remove(f)
                deleted_files.append(f)
            except Exception as e:
                print(f"Error deleting {f}: {e}")


def backup_old_files(base_dir, file_type):
    """
    Moves all existing PNG/PDF files to backup folder before saving new files.
    """
    if file_type.upper() == "PDF":
        src_dir = os.path.join(base_dir, "pdf_circle")
        bkp_dir = os.path.join(base_dir, "bkp_pdf_circle")
        ext = ".pdf"

    os.makedirs(bkp_dir, exist_ok=True)

    # Move all matching files to backup folder
    for file in os.listdir(src_dir):
        if file.lower().endswith(ext):
            src_file = os.path.join(src_dir, file)
            dst_file = os.path.join(bkp_dir, file)

            # Avoid overwriting: add timestamp if exists
            if os.path.exists(dst_file):
                name, extension = os.path.splitext(file)
                dst_file = os.path.join(bkp_dir, f"{name}{extension}")

            shutil.move(src_file, dst_file)

    print(f"Backup complete for {file_type}: {bkp_dir}")


if __name__ == "__main__":

    cyclone_rows = fetch_data.get_cyclone_data()
    if len(cyclone_rows):
        grouped_data = fetch_data.prepare_cyclone_data(cyclone_rows)

        # Backup PDF and PNG
        clean_output_folder()
        # base_dir = r"C:\inetpub\wwwroot\Weather\reports"
        # backup_old_files(base_dir, "PDF")
        # backup_old_files(base_dir, "PNG")

        # ---------- Generate PDF Report -------------
        html_path = render_html.build(grouped_data)
        pdf_path = export_pdf.convert(html_path)

        # ------------- Generate one-pager ------------------
        # one_pager_html_path = render_html.one_pager_build(circle, circle_name)
        # one_pager_img_path = export_pdf.convert_html_to_png(one_pager_html_path, circle)

        # ----------- Path PDF/PNG ---------------
        # abs_path_img = os.path.abspath(saved_files[0])
        # abs_path_pdf = os.path.abspath(pdf_path)

        # ----------- Send Whatsapp/Mail ---------------
        # send_email.send_with_attachment(pdf_path)
        # send_whatsapp.send_whatsapp_message(circle)

        # ----------- Open PDf in Browser  ---------------
        # webbrowser.open(f"file://{abs_path_pdf}")

        print("PDF generated and opened for preview.")

    else:
        print("No cyclone data for today.")

    # ----------- Send Whatsapp/Mail ---------------
    # for record in circle_list:
    #     circle = record["indus_circle"]
    #     circle_name = record["indus_circle_name"]

    #     now = datetime.now()
    #     today_str = now.strftime("%d %b %Y")

    #     output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "output"))
    #     os.makedirs(output_dir, exist_ok=True)

    #     png_path = os.path.join(output_dir, f"{circle} circle - {today_str}.png")
    #     pdf_path = os.path.join(output_dir, f"{circle} circle - {today_str}.pdf")

    #     pdf_file = os.path.abspath(pdf_path)
    #     png_path = os.path.abspath(png_path)

    #     print(output_dir, png_path)

    #     # send_email.send_with_attachment(pdf_file, png_path, circle, circle_name)
    #     # send_whatsapp.send_whatsapp_message(circle)
