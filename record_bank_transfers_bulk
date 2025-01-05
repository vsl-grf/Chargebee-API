import gspread
import csv
import os
import requests
from google.colab import auth
from google.auth import default
from google.colab import drive, files
from datetime import datetime
import pytz
import time

# Constants and Configuration
SHEET_URL = "https://docs.google.com/spreadsheets/d/xxx"
SHEET_NAME = "Workdesk"
TARGET_DATE = datetime.today().strftime("%d.%m.%Y")
LOG_FOLDER = '/content/drive/Shared drives/xxx'
URL_SHORTCUT_TARGET = "https:xxx.chargebee.com/bulk_operations"
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/xxx"

def setup_google_drive():
    """Mount Google Drive."""
    drive.mount('/content/drive', force_remount=True)
    print("Google Drive mounted.")

def authenticate_google_sheets():
    """Authenticate Google Sheets API and open the spreadsheet."""
    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)
    try:
        return gc.open_by_url(SHEET_URL)
    except gspread.SpreadsheetNotFound:
        raise FileNotFoundError("Spreadsheet not found. Check the URL or sharing settings.")
    except gspread.exceptions.APIError as e:
        raise RuntimeError(f"API Error: {e}")

def send_to_slack(message):
    """Send a message to a Slack channel using a webhook."""
    try:
        payload = {"text": message}
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            print("Message sent to Slack successfully.")
        else:
            print(f"Failed to send message to Slack. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error sending message to Slack: {e}")

def get_data_from_sheet(spreadsheet, sheet_name, start_time):
    """Retrieve and process data from the specified worksheet."""
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_values()

        headers = data[0]
        import_date_idx = headers.index("import_date")
        transaction_date_idx = headers.index("transaction_date")
        extracted_invoice_number_idx = headers.index("extracted_invoice_number")
        transferred_amount_idx = headers.index("transferred_amount")
        batch_number_idx = headers.index("batch_number")

        filtered_data = []
        total_amount_cents = 0
        batch_numbers = set()
        transaction_dates = []

        # Count all rows with TARGET_DATE in `import_date`
        total_entries_with_target_date = sum(1 for row in data[1:] if row[import_date_idx] == TARGET_DATE)

        for row in data[1:]:
            if row[import_date_idx] == TARGET_DATE and row[extracted_invoice_number_idx].startswith("IN0"):
                transaction_date = datetime.strptime(row[transaction_date_idx], "%d.%m.%Y").strftime("%Y-%m-%d 00:00:00")
                transaction_dates.append(transaction_date)
                amount_cents = int(float(row[transferred_amount_idx].replace(",", ".")) * 100)
                total_amount_cents += amount_cents
                batch_numbers.add(row[batch_number_idx])
                filtered_data.append([transaction_date, row[extracted_invoice_number_idx], amount_cents])

        sorted_data = sorted(filtered_data, key=lambda x: x[2], reverse=True)
        in0_count = len(filtered_data)
        accuracy_rate = (in0_count / total_entries_with_target_date) * 100 if total_entries_with_target_date else 0

        min_date = min(transaction_dates) if transaction_dates else "N/A"
        max_date = max(transaction_dates) if transaction_dates else "N/A"
        execution_time = time.time() - start_time

        slack_message = (
            f":bell: *New Bank Payment Batch Processed*\n"
            f":moneybag: *Batch Number:* {', '.join(sorted(batch_numbers))}\n"
            f":calendar: *Date Range:* {min_date} to {max_date}\n"
            f":moneybag: *Total Bank Payments Processed:* {in0_count}\n"
            f":dollar: *Total Amount (EUR):* {total_amount_cents / 100:.2f}\n"
            # f":dart: *Accuracy Rate:* {accuracy_rate:.2f}% _(Exact invoice matches \"IN0...\" / Total remittances in processed batch)_\n"
            ":star: *Top 3 Payments:*\n" +
            "\n".join([f"• *Date:* {entry[0]}, *Invoice ID:* {entry[1]}, *Amount (EUR):* {entry[2] / 100:.2f}" for entry in sorted_data[:3]]) +
            f"\n:hourglass_flowing_sand: *Execution Time:* {execution_time:.2f} seconds"
        )

        send_to_slack(slack_message)
        print(slack_message)
        print(f"Total results pulled: {len(filtered_data)}")
        print(f"Execution time: {execution_time:.2f} seconds")
        print(f"Accuracy Rate: {accuracy_rate:.2f}% --> IF LESS THAN 100%, GO BACK TO THE SHEET AND MATCH MANUALLY THE REMAINING ONES !!!")

        return filtered_data
    except Exception as e:
        raise RuntimeError(f"Error processing worksheet data: {e}")

def save_to_csv(file_name, data):
    """Save data to a CSV file."""
    try:
        if not os.path.exists(LOG_FOLDER):
            os.makedirs(LOG_FOLDER)
        file_path = os.path.join(LOG_FOLDER, file_name)
        with open(file_path, "w", newline="") as csv_file:
            writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
            writer.writerow(["transaction[date]", "invoice[id]", "transaction[amount]", "transaction[payment_method]"])
            for row in data:
                writer.writerow(row + ["bank_transfer"])
        print(f"Data successfully saved to {file_path}")
        files.download(file_path)
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

def create_url_shortcut(file_name, target_url):
    """Create a URL shortcut file."""
    try:
        with open(file_name, "w") as shortcut_file:
            shortcut_file.write(f"[InternetShortcut]\nURL={target_url}\n")
        print(f"Shortcut file created: {file_name}")
        files.download(file_name)
    except Exception as e:
        print(f"Error creating URL shortcut file: {e}")

def main():
    """Main script execution."""
    start_time = time.time()
    try:
        setup_google_drive()
        spreadsheet = authenticate_google_sheets()
        current_time_cet = datetime.now(pytz.timezone("CET")).strftime("%d%m%Y_%H%M%S")
        output_file = f"invoice_bulk_operation_{current_time_cet}.csv"
        shortcut_file = f"CB_link_to_upload_{current_time_cet}.url"

        filtered_data = get_data_from_sheet(spreadsheet, SHEET_NAME, start_time)
        save_to_csv(output_file, filtered_data)
        create_url_shortcut(shortcut_file, URL_SHORTCUT_TARGET)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
