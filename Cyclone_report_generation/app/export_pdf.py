import base64
import os
import shutil
from datetime import datetime
from pathlib import Path

import fitz
from playwright.sync_api import sync_playwright


def convert(html_path):
    # Prepare output filename
    now = datetime.now()
    today_str = now.strftime("%d %b %Y")

    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output"
    )
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"cyclone - {today_str}.pdf")

    # Convert HTML to file URL
    abs_html_path = os.path.abspath(html_path)
    url = f"file:///{abs_html_path.replace(os.sep, '/')}"

    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context()
        page = context.new_page()

        # Load HTML file
        page.goto(url, wait_until="load")

        # CDP session for PDF generation
        client = context.new_cdp_session(page)

        # Create PDF using Chrome DevTools Protocol
        pdf_data = client.send(
            "Page.printToPDF",
            {
                "printBackground": True,
                "displayHeaderFooter": True,
                # empty header
                # "headerTemplate": "<div></div>",
                # footer with page numbers
                "footerTemplate": """
                <div style="width:100%;padding: 5px 20px 2px 20px;position:relative;top:10px;border-top: 1px solid #dedede;">
                    <div
                        style="
                            display: flex;
                            justify-content: space-between;
                            align-items:center;
                            background-color:#000000 !important;
                            ">
                        <div>
                            <span style="font-size: 9px">Powered by:</span>
                            <span style="font-size: 11px; font-weight: 600;margin-left:8px;">ML INFOMAP PVT LTD</span>
                            <span style="font-size: 9px;margin-left:8px;">124-A, Katwaria Sarai, New Delhi-110016; Email:post@mlinfomap.com</span>
                        </div>
                        <div style='font-size:9px;'>
                            Page <span class="pageNumber" style="font-weight:600;font-size:11px;"></span> of <span class="totalPages" style="font-weight:600;font-size:11px;"></span>
                        </div>
                    </div>
                </div>
            """,
                # A4 size in inches
                "paperWidth": 8.27,
                "paperHeight": 11.69,
                # margins (in inches)
                "marginTop": 0.2,
                "marginBottom": 0.38,
                "marginLeft": 0.2,
                "marginRight": 0.2,
            },
        )

        # Write the PDF
        pdf_bytes = base64.b64decode(pdf_data["data"])
        with open(output_path, "wb") as f:
            f.write(pdf_bytes)

        browser.close()

        save_file_wwwroot(output_path, "PDF")

    return output_path


def save_file_wwwroot(file_path, file_type):
    base_dir = r"C:\inetpub\wwwroot\Weather\reports"

    if file_type.upper() == "PDF":
        report_dir = os.path.join(base_dir, "pdf_circle")
    else:
        report_dir = os.path.join(base_dir, "one_pager_circle")

    os.makedirs(report_dir, exist_ok=True)

    # Final destination path
    file_name = os.path.basename(file_path)
    destination_path = os.path.join(report_dir, file_name)

    shutil.copyfile(file_path, destination_path)
    return destination_path


def pdf_to_png(pdf_path, district, output_folder="output", zoom_x=1.5, zoom_y=1.5):
    os.makedirs(output_folder, exist_ok=True)
    doc = fitz.open(pdf_path)
    saved_files = []

    for i, page in enumerate(doc):
        matrix = fitz.Matrix(zoom_x, zoom_y)  # zoom factor (2.0 = 200%)
        pix = page.get_pixmap(matrix=matrix)
        # out_file = f"{output_folder}/page_{i+1}.png"
        out_file = f"{output_folder}/report.png"
        # url_file = save_file_wwwroot(district)
        pix.save(out_file)
        # pix.save(url_file)
        saved_files.append(out_file)

    return saved_files


def convert_html_to_png(html_path, circle):
    # Create absolute and output paths
    now = datetime.now()
    today_str = f"{now.strftime('%d %b %Y')}"
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output"
    )

    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f"{circle} circle - {today_str}.png")

    abs_html_path = os.path.abspath(html_path)
    url = f"file:///{abs_html_path.replace(os.sep, '/')}"  # Ensure forward slashes for file URLs

    with sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context(
            viewport={"width": 1080, "height": 2000},
            device_scale_factor=1,  # Increases sharpness & clarity
        )
        page = context.new_page()

        print(f"Loading: {url}")
        page.goto(url, wait_until="networkidle")

        # Get full scroll height of content
        content_height = page.evaluate("document.body.scrollHeight")

        # Resize viewport to match content
        page.set_viewport_size({"width": 1080, "height": content_height})

        page.screenshot(
            path=output_path,
            full_page=True,
            type="png",
        )

        browser.close()

        save_file_wwwroot(output_path, "PNG")

    print(f"PNG saved at: {output_path}")
    return output_path
