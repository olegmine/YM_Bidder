import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
from aiohttp import ClientError

from scr.config import (
   SAMPLE_SPREADSHEET_ID  , Tech_PC_Components_YM, B_id_Tech_PC_Components_YM, SSmart_shop_YM,
    B_id_SSmart_shop_YM, ByMarket_YM, B_id_ByMarket_YM
)
from scr.data_fetcher import get_sheet_data
from scr.data_writer import write_sheet_data
from scr.logger import logger
from scr.yandex_market_report import get_yandex_market_report
from scr.update_data_ym import compare_prices_and_create_for_update, update_dataframe
from scr.update_ym import update_price_ym

DEBUG = False

async def save_debug_csv(df: pd.DataFrame, filename: str) -> None:
    if DEBUG:
        try:
            await asyncio.to_thread(df.to_csv, filename, index=False)
            logger.debug(f"Сохранен отладочный CSV: {filename}")
        except IOError as e:
            logger.error(f"Ошибка при сохранении отладочного CSV {filename}: {str(e)}")

async def keep_specific_columns(df):
    columns_to_keep = [
        'SHOP_SKU', 'OFFER', 'MAIN_PRICE', 'MERCH_PRICE_WITH_PROMOS',
        'PRICE_GREEN_THRESHOLD', 'PRICE_RED_THRESHOLD', 'PRICE_WITH_PROMOS',
        'SHOP_WITH_BEST_PRICE_ON_MARKET', 'PRICE.1'
    ]
    return await asyncio.to_thread(lambda: df[columns_to_keep].dropna(subset=['PRICE.1']))

async def process_yandex_market_range(
        range_name: str,
        sheet_range: str,
        api_key: str,
        business_id: int,
        executor: ThreadPoolExecutor
) -> None:
    ym_logger = logger.bind(marketplace="YandexMarket", range=range_name)
    my_market = ['SSmart shop','Tech PC Components','ByMarket']
    column_names = {
        'seller_id': 'SHOP_SKU',
        'name': 'OFFER',
        'link': 'LINK',
        'price': 'MERCH_PRICE_WITH_PROMOS',
        'stop': 'STOP',
        'mp_on_market': 'PRICE.1',
        'market_with_mp': 'SHOP_WITH_BEST_PRICE_ON_MARKET',
        'prim': 'PRIM'
    }
    try:
        ym_logger.info("Начало обработки диапазона")

        # Получение данных из Google Sheets
        df: Optional[pd.DataFrame] = None
        try:
            df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, sheet_range)
        except Exception as e:
            ym_logger.error(f"Ошибка при получении данных из Google Sheets: {str(e)}")
            return

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        await save_debug_csv(df, f"report/{range_name}{current_time}_first.csv")

        # Получение отчета с Яндекс.Маркета
        ym_report_df: Optional[pd.DataFrame] = None
        try:
            ym_report_df = await get_yandex_market_report(api_key, business_id)
            ym_report_df = await keep_specific_columns(ym_report_df)
            # ym_report_df = ym_report_df.dropna(subset=['PRICE.1'])
            await save_debug_csv(ym_report_df, f"report/{range_name}{current_time}_ym_report.csv")

        except Exception as e:
            ym_logger.error(f"Ошибка при получении отчета с Яндекс.Маркета: {str(e)}")
            return

        # Обновление и сравнение данных
        try:


            updated_df = await update_dataframe(df, ym_report_df,column_names)
            # print(ym_report_df.info())
            updated_df, for_update_df = await compare_prices_and_create_for_update(updated_df,column_names,my_market)
            await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, sheet_range.replace('1', '3'))
        except Exception as e:
            ym_logger.error(f"Ошибка при обновлении и сравнении данных: {str(e)}")
            return

        # Обновление цен через API
        if not for_update_df.empty :
            print(for_update_df.info())
            ym_logger.warning("Начало обновления цен через API", importance="high")
            try:
                await update_price_ym(for_update_df, api_key, business_id, "SHOP_SKU",
                                      "MERCH_PRICE_WITH_PROMOS",'discount_base' ,debug=DEBUG)
                ym_logger.warning("Завершено обновление цен через API")
            except ClientError as e:
                ym_logger.error(f"Ошибка при обновлении цен через API: {str(e)}")
            except Exception as e:
                ym_logger.error(f"Неожиданная ошибка при обновлении цен: {str(e)}")

        try:
            await save_debug_csv(updated_df, f"report_ym/{range_name}{current_time}_updated.csv")
            await save_debug_csv(for_update_df, f"report_ym/{range_name}{current_time}_for_update.csv")
        except:
            logger.info('Не удалось сохранить один из датафреймов')

        ym_logger.info(f"Обработка диапазона {range_name} завершена")
    except Exception as e:
        ym_logger.error(f"Критическая ошибка при обработке диапазона {range_name}: {str(e)}", exc_info=True)

async def update_data_ym() -> None:
    ym_logger = logger.bind(marketplace="YandexMarket")

    try:
        ym_logger.warning("Начало обновления данных Yandex Market")
        ym_ranges: List[Tuple[str, str, str, int]] = [
            ('Tech PC Components', 'YM_Tech_PC!A1:L', Tech_PC_Components_YM, B_id_Tech_PC_Components_YM),
            ('ByMarket', 'YM_ByMarket!A1:L', ByMarket_YM, B_id_ByMarket_YM),
            ('SSmart_shop', 'YM_SSmart_Shop!A1:L', SSmart_shop_YM, B_id_SSmart_shop_YM)
        ]

        with ThreadPoolExecutor() as executor:
            for i, (range_name, sheet_range, api_key, business_id) in enumerate(ym_ranges):
                if i > 0:
                    pause_duration = random.uniform(1 * 60, 1 * 60)
                    ym_logger.warning(f"Пауза перед обработкой {range_name}: {pause_duration / 60:.2f} минут")
                    await asyncio.sleep(pause_duration)

                await process_yandex_market_range(range_name, sheet_range, api_key, business_id, executor)

        ym_logger.info("Обновление данных Yandex Market успешно завершено")
    except Exception as e:
        ym_logger.error(f"Критическая ошибка при обновлении данных Yandex Market: {str(e)}", exc_info=True)

async def update_loop() -> None:
    while True:
        try:
            logger.info("Начало цикла обновления данных для Yandex Market")
            await update_data_ym()
            logger.info("Цикл обновления данных для Yandex Market успешно завершен")
        except Exception as e:
            logger.warning(f"Критическая ошибка в цикле обновления данных: {str(e)}")
        logger.warning(f"Ожидание 30 минут до следующего обновления")
        await asyncio.sleep(30 * 60)

async def main() -> None:
    await update_loop()

if __name__ == "__main__":
    asyncio.run(main())