import asyncio
import base64
from pathlib import Path

# גודל כל חתיכה בבייטים. 1024 בטוח מאוד ל-UDP
CHUNK_SIZE = 1024


class UDPClientProtocol:
    def __init__(self):
        # הפעם הפרוטוקול לא מקבל רשימת קבצים מראש
        self.transport = None
        self.loop = asyncio.get_running_loop()
        # נוסיף 'מנעול' (Event) כדי למנוע שליחה של שני קבצים במקביל
        self.send_in_progress = asyncio.Event()
        self.send_in_progress.set()  # מתחיל במצב "פנוי"

    def connection_made(self, transport):
        self.transport = transport
        print("Client connected to server. Ready to send files.")
        # אנחנו *לא* קוראים אוטומטית ל-send_files

    async def send_file(self, file_path: Path):
        # נחכה אם קובץ אחר כבר בתהליך שליחה
        await self.send_in_progress.wait()
        # ננעל את המנעול (מצב "עסוק")
        self.send_in_progress.clear()

        filename = file_path.name
        print(f"--- Sending file: {filename} ---")

        try:
            # 1. שלח הודעת START
            start_msg = f"START|{filename}".encode('utf-8')
            self.transport.sendto(start_msg)

            # 2. קרא את הקובץ ושלח חתיכות DATA
            chunk_index = 0
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break  # סוף הקובץ

                    chunk_b64 = base64.b64encode(chunk).decode('utf-8')
                    data_msg = f"DATA|{filename}|{chunk_b64}".encode('utf-8')
                    self.transport.sendto(data_msg)

                    if chunk_index % 100 == 0:
                        print(f"[{filename}] Sent chunk {chunk_index}...")
                    chunk_index += 1

                    await asyncio.sleep(0.0001)

            # 3. שלח הודעת END
            end_msg = f"END|{filename}".encode('utf-8')
            self.transport.sendto(end_msg)
            print(f"[{filename}] Sent END. Total {chunk_index} chunks.")

        except FileNotFoundError:
            print(f"ERROR: File not found at {file_path}")
        except Exception as e:
            print(f"ERROR sending file {file_path.name}: {e}")
        finally:
            # שחרור המנעול (מצב "פנוי") בכל מקרה
            self.send_in_progress.set()

    def datagram_received(self, data, addr):
        print(f"Received from server: {data.decode()}")

    def connection_lost(self, exc):
        print("Socket closed")


async def main():
    loop = asyncio.get_running_loop()

    # עדיין צריך לדעת איפה לחפש את הקבצים
    base_path = Path(r'C:\Users\j4aco\PycharmProjects\AlmogProject\files')
    print(f"Looking for files in: {base_path}")

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(),  # הפרוטוקול נוצר בלי רשימת קבצים
        remote_addr=('127.0.0.1', 9999)
    )

    try:
        # לולאה אינטראקטיבית לקבלת קלט מהמשתמש
        while True:
            # הרצת input() ב-executor כדי לא לחסום את הלולאה
            prompt = "\n(Type 'exit' to quit)\nEnter file name to send: "
            filename = await loop.run_in_executor(
                None,  # השתמש ב-ThreadPoolExecutor הדיפולטי
                input,  # הפונקציה החוסמת
                prompt  # הארגומנט שיועבר ל-input
            )

            if filename.lower() == 'exit':
                break

            if not filename:
                continue

            file_path = base_path / filename

            # בדיקה אם הקובץ קיים לפני שמתחילים
            if not file_path.exists():
                print(f"File not found: {file_path}")
                continue

            # קריאה לפונקציה החדשה בפרוטוקול
            await protocol.send_file(file_path)

    except (asyncio.CancelledError, KeyboardInterrupt):
        print("\nClient shutting down...")
    finally:
        print("Closing connection.")
        if transport and not transport.is_closing():
            transport.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Client stopped by user.")