import asyncio
import base64
from pathlib import Path

# --- הגדרות קבועים ---
CHUNK_SIZE = 512  # גודל כל חתיכה
WINDOW_SIZE = 50  # (Selective Repeat) גודל החלון
RETRANSMIT_TIMEOUT = 1.0  # זמן המתנה ל-ACK לפני שליחה חוזרת
MAX_handshake_RETRIES = 5  # כמה פעמים לנסות לשלוח START לפני שנכשלים


class UDPClientProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        super().__init__()
        self.transport = None
        self.loop = asyncio.get_running_loop()

        # ניהול מצב שליחה
        self.send_in_progress = asyncio.Event()
        self.send_in_progress.set()

        self.current_send_job = None

        # אירועים (Events) לניהול זרימה
        self.ack_start_event = asyncio.Event()  # <--- חדש: לוודא שהשרת מוכן לקבל
        self.ack_event = asyncio.Event()  # מתי הגיע ACK כלשהו (כדי להזיז חלון)
        self.final_ack_event = asyncio.Event()  # מתי הקובץ הסתיים בהצלחה

        self.base_path = None

    def connection_made(self, transport):
        self.transport = transport
        print("Client connected to server. Ready to send files.")

    def connection_lost(self, exc):
        print("Socket closed")
        if self.current_send_job:
            for timer in self.current_send_job['timers'].values():
                timer.cancel()
            self.current_send_job = None

    def datagram_received(self, data, addr):
        message = data.decode()
        # print(f"Received from server: {message}") # אפשר להחזיר לדיבאג
        parts = message.split('|')
        command = parts[0]
        filename = parts[1]

        job = self.current_send_job
        # ודא שהתשובה קשורה למשימה הנוכחית (אלא אם כן זה ACK_START שיכול להגיע לפני שהגדרנו הכל)
        if not job or job['filename'] != filename:
            return

        if command == 'ACK_START':
            print(f"✅ [{filename}] Handshake successful (ACK_START received).")
            self.ack_start_event.set()

        elif command == 'ACK_DATA':
            seq_num = int(parts[2])
            if seq_num not in job['acks_received']:
                job['acks_received'].add(seq_num)
                if seq_num in job['timers']:
                    job['timers'][seq_num].cancel()
                    del job['timers'][seq_num]
                self.ack_event.set()

        elif command == 'ACK_END':
            print(f"✅ [{filename}] Transfer Confirmed by Server.")
            self.final_ack_event.set()

        elif command == 'NACK_END':
            print(f"❌ [{filename}] Received NACK. Resending missing chunks...")
            missing_chunks_str = parts[3]
            if missing_chunks_str:
                missing_chunks = [int(s) for s in missing_chunks_str.split(',')]
                for seq_num in missing_chunks:
                    if seq_num < len(job['chunks']):
                        self._send_chunk(seq_num)

            # נסה שוב לסיים
            self.loop.call_later(RETRANSMIT_TIMEOUT, self._send_end_message)

    def _send_chunk(self, seq_num):
        """שליחת חבילה בודדת וקביעת טיימר"""
        if not self.current_send_job: return
        job = self.current_send_job

        if seq_num in job['acks_received']:
            return

        try:
            filename = job['filename']
            chunk_data = job['chunks'][seq_num]
            chunk_b64 = base64.b64encode(chunk_data).decode('utf-8')

            message = f"DATA|{filename}|{seq_num}|{chunk_b64}".encode('utf-8')
            self.transport.sendto(message)

            # ניהול טיימר
            if seq_num in job['timers']:
                job['timers'][seq_num].cancel()

            job['timers'][seq_num] = self.loop.call_later(
                RETRANSMIT_TIMEOUT,
                self._send_chunk,
                seq_num
            )
        except Exception as e:
            print(f"Error sending chunk {seq_num}: {e}")

    def _send_end_message(self):
        """שליחת בקשת סיום"""
        if not self.current_send_job: return
        job = self.current_send_job

        print(f"[{job['filename']}] Sending END request (Total {job['total_chunks']})...")
        end_msg = f"END|{job['filename']}|{job['total_chunks']}".encode('utf-8')
        self.transport.sendto(end_msg)

        if job['end_timer']:
            job['end_timer'].cancel()

        job['end_timer'] = self.loop.call_later(
            RETRANSMIT_TIMEOUT * 2,
            self._send_end_message
        )

    async def send_file(self, file_path: Path):
        await self.send_in_progress.wait()
        self.send_in_progress.clear()

        # איפוס אירועים
        self.ack_start_event.clear()
        self.final_ack_event.clear()

        filename = file_path.name
        print(f"--- Preparing to send file: {filename} ---")

        try:
            # 1. קריאת הקובץ
            chunks = []
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk: break
                    chunks.append(chunk)

            if not chunks:
                print(f"[{filename}] File is empty.")
                return

            total_chunks = len(chunks)
            print(f"[{filename}] Loaded {total_chunks} chunks.")

            # 2. אתחול המשימה
            self.current_send_job = {
                'filename': filename,
                'chunks': chunks,
                'total_chunks': total_chunks,
                'acks_received': set(),
                'timers': {},
                'base': 0,
                'next_seq_num': 0,
                'end_timer': None
            }
            job = self.current_send_job

            # 3. === Handshake: שליחת START והמתנה ל-ACK ===
            print(f"[{filename}] Sending START command...")
            retry_count = 0
            handshake_success = False

            while retry_count < MAX_handshake_RETRIES:
                self.transport.sendto(f"START|{filename}".encode())
                try:
                    await asyncio.wait_for(self.ack_start_event.wait(), timeout=1.0)
                    handshake_success = True
                    break
                except asyncio.TimeoutError:
                    retry_count += 1
                    print(f"[{filename}] No ACK_START. Retrying handshake ({retry_count}/{MAX_handshake_RETRIES})...")

            if not handshake_success:
                print(f"❌ [{filename}] Server not responding to START. Aborting.")
                return  # יציאה מהפונקציה אם אין קשר

            # 4. === שליחת הנתונים (Selective Repeat) ===
            print(f"[{filename}] Starting Data Transmission...")
            while len(job['acks_received']) < total_chunks:
                # מילוי החלון
                while job['next_seq_num'] < total_chunks and \
                        job['next_seq_num'] < job['base'] + WINDOW_SIZE:
                    self._send_chunk(job['next_seq_num'])
                    job['next_seq_num'] += 1

                # הזזת החלון
                while job['base'] in job['acks_received']:
                    job['base'] += 1
                    if job['base'] == total_chunks: break

                if job['base'] == total_chunks: break

                # המתנה ל-ACK
                self.ack_event.clear()
                try:
                    await asyncio.wait_for(self.ack_event.wait(), timeout=0.1)
                except asyncio.TimeoutError:
                    pass

            print(f"[{filename}] All chunks sent and ACKed.")

            # 5. === סיום ===
            self._send_end_message()

            try:
                await asyncio.wait_for(self.final_ack_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                print(f"[{filename}] Warning: No final confirmation (ACK_END) received.")

        except FileNotFoundError:
            print(f"ERROR: File not found {file_path}")
        except Exception as e:
            print(f"ERROR sending file: {e}")
        finally:
            # ניקוי
            if self.current_send_job:
                for timer in self.current_send_job['timers'].values():
                    timer.cancel()
                if self.current_send_job['end_timer']:
                    self.current_send_job['end_timer'].cancel()

            self.current_send_job = None
            self.send_in_progress.set()
            print(f"--- [{filename}] Job Finished ---")


async def main():
    loop = asyncio.get_running_loop()

    # שים לב: וודא שהנתיב הזה קיים במחשב שלך
    base_path = Path(r'C:\Users\j4aco\PycharmProjects\AlmogProject\files')
    if not base_path.exists():
        print(f"Warning: Base path {base_path} does not exist.")

    print(f"Looking for files in: {base_path}")

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(),
        remote_addr=('127.0.0.1', 9999)
    )
    protocol.base_path = base_path

    try:
        while True:
            prompt = "\n(Type 'exit' to quit)\nEnter file name to send: "
            filename = await loop.run_in_executor(None, input, prompt)
            filename = filename.strip()

            if filename.lower() == 'exit':
                break
            if not filename:
                continue

            file_path = base_path / filename
            if not file_path.exists():
                print(f"❌ File not found: {file_path}")
                continue

            await protocol.send_file(file_path)

    except KeyboardInterrupt:
        print("\nClient stopping...")
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())