import asyncio
import base64
from pathlib import Path

# איפה לשמור קבצים שהתקבלו
OUTPUT_DIR = Path(r'C:\Users\j4aco\PycharmProjects\AlmogProject\files\received')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class UDPServerProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        super().__init__()
        # מילון שיחזיק את כל החלקים של הקבצים שמגיעים
        # 'filename.txt' -> {0: chunk0, 1: chunk1, 3: chunk3, ...}
        self.file_buffers = {}

    def connection_made(self, transport):
        self.transport = transport
        print(f"Server started, waiting for files on port 9999...")
        print(f"Files will be saved in: {OUTPUT_DIR}")

    def datagram_received(self, data, addr):
        try:
            message = data.decode('utf-8')
            parts = message.split('|', 3)  # פיצול מקסימלי של 4 חלקים
            command = parts[0]
            FileName = parts[1]

            if command == 'START':
                print(f"[{FileName}] Received START from {addr}")
                # אתחול באפר חדש (כמילון) עבור הקובץ
                self.file_buffers[FileName] = {}
                self.transport.sendto(f"ACK_START|{FileName}".encode(), addr)

            elif command == 'DATA':
                seq_num = int(parts[2])
                chunk_data_b64 = parts[3]

                if FileName in self.file_buffers:
                    # בדוק אם כבר קיבלנו את החבילה הזו (למקרה שה-ACK שלנו אבד)
                    if seq_num not in self.file_buffers[FileName]:
                        chunk_data = base64.b64decode(chunk_data_b64) # פענוח הבינארי Presentation
                        # שמור את החתיכה הבינארית במילון לפי המספר הסידורי
                        self.file_buffers[FileName][seq_num] = chunk_data # Transport Layer
                        # print(f"[{filename}] Received chunk {seq_num}") # (יכול להציף את הלוג)

                    # שלח ACK ספציפי עבור חבילה זו, גם אם היא כפולה
                    # (כדי לטפל במקרה שה-ACK הקודם אבד)
                    self.transport.sendto(f"ACK_DATA|{FileName}|{seq_num}".encode(), addr)
                else:
                    print(f"[{FileName}] Received DATA without START, ignoring.")

            elif command == 'END':
                total_chunks = int(parts[2])  # הקליינט יגיד לנו כמה לצפות
                print(f"[{FileName}] Received END from {addr} (expecting {total_chunks} chunks)")
                if FileName in self.file_buffers:
                    self.save_file(FileName, total_chunks, addr)
                else:
                    print(f"[{FileName}] Received END without START, ignoring.")

        except Exception as e:
            print(f"Error processing datagram: {e}")

    def save_file(self, FileName, total_chunks, addr):
        try:
            received_chunks_map = self.file_buffers[FileName]

            # בדוק אם יש לנו את כל החלקים
            if len(received_chunks_map) != total_chunks:
                # --- Selective Repeat: שלח NACK (Negative Ack) ---
                missing_chunks = []
                for i in range(total_chunks):
                    if i not in received_chunks_map:
                        missing_chunks.append(str(i))

                missing_str = ",".join(missing_chunks)
                print(f"❌ [{FileName}] Missing {len(missing_chunks)} chunks. Sending NACK_END.")
                self.transport.sendto(f"NACK_END|{FileName}|missing|{missing_str}".encode(), addr)
                # אל תמחק את הבאפר! חכה שהקליינט ישלח את החסרים
                return

            # הרכבת הקובץ מכל החלקים *לפי הסדר*
            full_data_list = []
            for i in range(total_chunks):
                full_data_list.append(received_chunks_map[i])

            full_data = b''.join(full_data_list)
            output_path = OUTPUT_DIR / f"received_{FileName}"

            # כתיבת הקובץ
            with open(output_path, 'wb') as f:
                f.write(full_data)

            file_size_mb = len(full_data) / (1024 * 1024)
            print(f"✅ [{FileName}] File saved successfully to {output_path} ({file_size_mb:.2f} MB)")

            # שליחת אישור סופי
            self.transport.sendto(f"ACK_END|{FileName}|{file_size_mb:.2f}MB".encode(), addr)

        except KeyError as e:
            # זה יקרה אם `total_chunks` היה 100 אבל חסרה חבילה 50
            print(f"Error saving file {FileName}: Missing chunk {e}")
            self.transport.sendto(f"ERROR_SAVE|{FileName}|Missing chunk {e}".encode(), addr)
        except Exception as e:
            print(f"Error saving file {FileName}: {e}")
            self.transport.sendto(f"ERROR_SAVE|{FileName}|{e}".encode(), addr)

        finally:
            # ניקוי הבאפר רק אם ההרכבה הצליחה
            if FileName in self.file_buffers and f"ACK_END" in locals().get('ack_msg', ''):
                del self.file_buffers[FileName]

async def main():
    loop = asyncio.get_running_loop()
    # יצירת ה-Socket
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(),
        local_addr=('127.0.0.1', 9999) )

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("Server closing...")
        transport.close()

if __name__ == "__main__":
    asyncio.run(main())