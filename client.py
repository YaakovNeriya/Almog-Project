import asyncio
import base64
from pathlib import Path
# -
# גודל כל חתיכה בבייטים. 1024 בטוח מאוד ל-UDP
CHUNK_SIZE = 1024


class UDPClientProtocol:
    def __init__(self, file_paths):
        self.file_paths = file_paths
        self.transport = None
        self.loop = asyncio.get_running_loop()

    def connection_made(self, transport):
        self.transport = transport
        print("Client connected to server")
        # אנחנו לא רוצים לחסום את connection_made,
        # אז אנחנו יוצרים "משימה" (task) א-סינכרונית שתרוץ ברקע
        self.loop.create_task(self.send_files())

    async def send_files(self):
        for file_path in self.file_paths:
            filename = file_path.name
            print(f"--- Sending file: {filename} ---")

            try:
                # 1. שלח הודעת START
                start_msg = f"START|{filename}".encode('utf-8')
                self.transport.sendto(start_msg)

                # 2. קרא את הקובץ ושלח חתיכות DATA
                chunk_index = 0
                # חשוב: פתיחה במצב 'rb' (read binary)
                with open(file_path, 'rb') as f:
                    while True:
                        chunk = f.read(CHUNK_SIZE)
                        if not chunk:
                            break  # סוף הקובץ

                        # קודד את החתיכה הבינארית ל-Base64 (שהוא טקסט)
                        chunk_b64 = base64.b64encode(chunk).decode('utf-8')

                        data_msg = f"DATA|{filename}|{chunk_b64}".encode('utf-8')
                        self.transport.sendto(data_msg)

                        if chunk_index % 100 == 0:  # הדפס עדכון כל 100 חבילות
                            print(f"[{filename}] Sent chunk {chunk_index}...")

                        chunk_index += 1

                        # --- חשוב מאוד ---
                        # תן "נשימה" קטנה ללולאת האירועים.
                        # זה מונע הצפה של באפר ה-UDP של מערכת ההפעלה.
                        await asyncio.sleep(0.0001)

                        # 3. שלח הודעת END
                end_msg = f"END|{filename}".encode('utf-8')
                self.transport.sendto(end_msg)
                print(f"[{filename}] Sent END. Total {chunk_index} chunks.")

            except FileNotFoundError:
                print(f"ERROR: File not found at {file_path}")
            except Exception as e:
                print(f"ERROR sending file {file_path.name}: {e}")

        # תן עוד כמה שניות לקבל תשובות מהשרת לפני סגירה
        await asyncio.sleep(2)
        print("All files sent. Closing client.")
        self.transport.close()

    def datagram_received(self, data, addr):
        print(f"Received from server: {data.decode()}")

    def connection_lost(self, exc):
        print("Socket closed")


async def main():
    loop = asyncio.get_running_loop()

    base_path = Path(r'C:\Users\j4aco\PycharmProjects\AlmogProject\files')
    files_to_send = [
        base_path / 'data1.txt',
        base_path / 'data2.txt'
    ]

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(files_to_send),
        remote_addr=('127.0.0.1', 9999)
    )

    # הפרוטוקול עצמו יסגור את ה-transport כשיסיים לשלוח
    # אנחנו רק צריכים לחכות שהפרוטוקול יגמר
    # ניתן ל-transport "להיסגר" באופן טבעי
    try:
        # המתנה ארוכה כדי לאפשר לפרוטוקול לרוץ
        # הפרוטוקול יסגור את ה-transport בעצמו
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass  # הלולאה נסגרת מבחוץ
    finally:
        if transport and not transport.is_closing():
            transport.close()


if __name__ == "__main__":
    # כדי לאפשר לחיצה על Ctrl+C לעצור את הקליינט
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Client stopped by user.")