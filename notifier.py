# -*- coding: utf-8 -*-

import os

import requests

import resend

import pandas as pd

from datetime import datetime, timedelta



class StockNotifier:

    def __init__(self):

        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")

        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")

        self.resend_api_key = os.getenv("RESEND_API_KEY")

        

        if self.resend_api_key:

            resend.api_key = self.resend_api_key



    def get_now_time_str(self):

        """獲取 UTC+8 時間字串"""

        # GitHub Actions 伺服器通常是 UTC，手動加 8 小時

        now_utc8 = datetime.utcnow() + timedelta(hours=8)

        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")



    def send_telegram(self, message):

        """發送 Telegram 即時通知"""

        if not self.tg_token or not self.tg_chat_id:

            return False

        

        # 訊息末尾加上時間戳記

        ts = self.get_now_time_str().split(" ")[1]

        full_message = f"{message}\n\n🕒 <i>Sent at {ts} (UTC+8)</i>"

        

        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"

        payload = {"chat_id": self.tg_chat_id, "text": full_message, "parse_mode": "HTML"}

        try:

            requests.post(url, json=payload, timeout=10)

            return True

        except:

            return False



    def send_stock_report(self, market_name, img_data, report_df, text_reports, stats=None):

        """

        🚀 核心方法：完全對接 main.py 第 66 行的呼叫

        """

        if not self.resend_api_key:

            print("⚠️ 缺少 Resend API Key，無法寄信。")

            return False



        report_time = self.get_now_time_str()

        

        # 解析統計數據 (從 stats 字典獲取，若無則從 report_df 估算)

        total_count = stats.get('total', 'N/A') if stats else 'N/A'

        success_count = stats.get('success', len(report_df)) if stats else len(report_df)

        fail_count = stats.get('fail', 0) if stats else 0

        success_rate = f"{(success_count/total_count)*100:.1f}%" if isinstance(total_count, int) and total_count > 0 else "N/A"



        subject = f"📊 {market_name} 全方位市場監控報表 - {report_time.split(' ')[0]}"

        

        # 建立 HTML 郵件內容

        html_content = f"""

        <html>

        <body style="font-family: 'Microsoft JhengHei', sans-serif; color: #333;">

            <div style="max-width: 700px; margin: auto; border: 1px solid #ddd; border-top: 10px solid #28a745; border-radius: 10px; padding: 25px;">

                <h2 style="color: #1a73e8; border-bottom: 2px solid #eee; padding-bottom: 10px;">{market_name} 市場監控報告</h2>

                <p style="color: #666;">報告生成時間: <b>{report_time} (台北時間 UTC+8)</b></p>

                

                <table style="width: 100%; border-collapse: collapse; margin-top: 20px; background-color: #f9f9f9;">

                    <tr style="background-color: #e8f0fe;">

                        <th style="padding: 12px; border: 1px solid #ccc; text-align: left;">統計項目</th>

                        <th style="padding: 12px; border: 1px solid #ccc; text-align: left;">數據內容</th>

                    </tr>

                    <tr>

                        <td style="padding: 10px; border: 1px solid #ccc;">應收標的總數</td>

                        <td style="padding: 10px; border: 1px solid #ccc; font-weight: bold;">{total_count}</td>

                    </tr>

                    <tr>

                        <td style="padding: 10px; border: 1px solid #ccc;">成功更新數量</td>

                        <td style="padding: 10px; border: 1px solid #ccc; color: #28a745; font-weight: bold;">{success_count}</td>

                    </tr>

                    <tr>

                        <td style="padding: 10px; border: 1px solid #ccc;">失敗/無數據</td>

                        <td style="padding: 10px; border: 1px solid #ccc; color: #dc3545;">{fail_count}</td>

                    </tr>

                    <tr>

                        <td style="padding: 10px; border: 1px solid #ccc;">今日成功率</td>

                        <td style="padding: 10px; border: 1px solid #ccc; font-weight: bold;">{success_rate}</td>

                    </tr>

                </table>



                <div style="margin-top: 30px; padding: 15px; background-color: #fff3cd; border-left: 5px solid #ffc107;">

                    <strong>系統通知：</strong><br>

                    數據分析已完成。本次掃描包含上市、上櫃及各類 ETF 標的。圖表附件已生成於系統目錄。

                </div>

                

                <p style="margin-top: 40px; font-size: 12px; color: #999; text-align: center; border-top: 1px solid #eee; padding-top: 10px;">

                    此郵件由 Global Stock Matrix Monitor 系統自動發送

                </p>

            </div>

        </body>

        </html>

        """



        try:

            # 發送郵件 (固定寄給你的 Gmail 以符合 Resend 測試限制)

            resend.Emails.send({

                "from": "StockMatrix <onboarding@resend.dev>",

                "to": "grissomlin643@gmail.com",

                "subject": subject,

                "html": html_content

            })

            

            # 同步發送 Telegram 簡報

            tg_msg = f"📊 <b>{market_name} 監控報表已送達</b>\n成功率: {success_rate}\n更新: {success_count} 檔"

            self.send_telegram(tg_msg)

            

            return True

        except Exception as e:

            print(f"❌ 郵件發送失敗: {e}")

            return False

???那原來的九張圖呢?????
