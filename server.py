import asyncio
import base64
from pathlib import Path

# איפה לשמור קבצים שהתקבלו
OUTPUT_DIR = Path(r'C:\Users\j4aco\PycharmProjects\AlmogProject\files\received')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # יוצר את התיקייה אם לא קיימת


class UDPServerProtocol:
    def __init__(self):
        # מילון שיחזיק את כל החלקים של הקבצים שמגיעים
        # 'filename.txt' -> [chunk1, chunk2, chunk3, ...]
        self.file_buffers = {}

    def connection_made(self, transport):
        self.transport = transport
        print(f"Server started, waiting for files on port 9999...")
        print(f"Files will be saved in: {OUTPUT_DIR}")

    def datagram_received(self, data, addr):
        try:
            message = data.decode('utf-8')
            parts = message.split('|', 2)  # פיצול מקסימלי של 3 חלקים
            command = parts[0]
            filename = parts[1]

            if command == 'START':
                print(f"[{filename}] Received START from {addr}")
                # אתחול באפר חדש (כרשימה) עבור הקובץ
                self.file_buffers[filename] = []
                self.transport.sendto(f"ACK_START|{filename}".encode(), addr)

            elif command == 'DATA':
                if filename in self.file_buffers:
                    # קח את החלק השלישי (המידע) ובצע דיקוד מ-Base64
                    chunk_data_b64 = parts[2]
                    chunk_data = base64.b64decode(chunk_data_b64)

                    # הוסף את החתיכה הבינארית לרשימה
                    self.file_buffers[filename].append(chunk_data)
                    # הערה: אנחנו *לא* שולחים ACK על כל חבילה כדי שזה יהיה מהיר
                    # (וזו גם הסיבה שזה לא אמין)
                else:
                    print(f"[{filename}] Received DATA without START, ignoring.")

            elif command == 'END':
                print(f"[{filename}] Received END from {addr}")
                if filename in self.file_buffers:
                    self.save_file(filename, addr)
                else:
                    print(f"[{filename}] Received END without START, ignoring.")

        except Exception as e:
            print(f"Error processing datagram: {e}")

    def save_file(self, filename, addr):
        # הרכבת הקובץ מכל החלקים
        try:
            full_data = b''.join(self.file_buffers[filename])
            output_path = OUTPUT_DIR / f"received_{filename}"

            # כתיבת הקובץ (במצב בינארי 'wb')
            with open(output_path, 'wb') as f:
                f.write(full_data)

            file_size_mb = len(full_data) / (1024 * 1024)
            print(f"✅ [{filename}] File saved successfully to {output_path} ({file_size_mb:.2f} MB)")

            # שליחת אישור סופי
            self.transport.sendto(f"ACK_END|{filename}|{file_size_mb:.2f}MB".encode(), addr)

        except Exception as e:
            print(f"Error saving file {filename}: {e}")
            self.transport.sendto(f"ERROR_SAVE|{filename}|{e}".encode(), addr)

        finally:
            # ניקוי הבאפר בין אם הצליח או נכשל
            if filename in self.file_buffers:
                del self.file_buffers[filename]


async def main():
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(),
        local_addr=('127.0.0.1', 9999)
    )

    try:
        await asyncio.Event().wait()  # דרך מודרנית להשאיר את השרת רץ
    except KeyboardInterrupt:
        print("Server closing...")
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())