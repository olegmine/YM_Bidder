import pandas as pd
import numpy as np
import logging
import random
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict
from scr.logger import logger

# Настройка логирования
logger = logger

# Создаем глобальный ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=4)


async def run_in_executor(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, func, *args)


async def update_dataframe(df1: pd.DataFrame, df2: pd.DataFrame, column_names: Dict[str, str]) -> pd.DataFrame:
    """
    Асинхронно обновляет первый DataFrame данными из второго DataFrame на основе seller_id.

    :param df1: Первый DataFrame
    :param df2: Второй DataFrame
    :param column_names: Словарь с названиями колонок
    :return: Обновленный DataFrame
    """

    def update_df():
        df1_updated = df1.copy()
        df2_updated = df2.copy()

        seller_id = column_names['seller_id']
        mp_on_market = column_names['mp_on_market']
        market_with_mp = column_names['market_with_mp']

        df1_updated[seller_id] = df1_updated[seller_id].astype(str)
        df2_updated[seller_id] = df2_updated[seller_id].astype(str)

        merged_df = df1_updated.merge(df2_updated[[seller_id, mp_on_market, market_with_mp]],
                                      on=seller_id,
                                      how='left',
                                      suffixes=('', '_new'))

        merged_df[mp_on_market] = merged_df[f'{mp_on_market}_new'].fillna(merged_df[mp_on_market])
        merged_df[market_with_mp] = merged_df[f'{market_with_mp}_new'].fillna(merged_df[market_with_mp])

        merged_df = merged_df.drop([f'{mp_on_market}_new', f'{market_with_mp}_new'], axis=1)

        original_type = df1[seller_id].dtype
        merged_df[seller_id] = merged_df[seller_id].astype(original_type)

        return merged_df

    return await run_in_executor(update_df)


async def compare_prices_and_create_for_update(df: pd.DataFrame, column_names: Dict[str, str], my_market: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Асинхронно сравнивает цены и создает DataFrame для обновления.

    :param df: Исходный DataFrame
    :param column_names: Словарь с названиями колонок
    :param my_market: Список названий ваших магазинов
    :return: Кортеж из обновленного DataFrame и DataFrame для обновления
    """
    try:
        updated_df = df.copy()
        updated_df[column_names['prim']] = ''

        numeric_columns = [column_names['price'], column_names['mp_on_market'], column_names['stop']]
        for col in numeric_columns:
            updated_df[col] = pd.to_numeric(updated_df[col], errors='coerce')

        # Проверка на пустые значения в колонке'stop'
        empty_stop_mask = updated_df[column_names['stop']].isna()
        updated_df.loc[empty_stop_mask, column_names['prim']] = "Пустое значение в колонке 'stop'"

        nan_mask = updated_df[numeric_columns].isna().any(axis=1)
        if nan_mask.any():
            logger.warning(f"Обнаружены NaN значения в {nan_mask.sum()} строках")
            logger.warning(updated_df[nan_mask].to_string())

        mask = (updated_df[column_names['price']] > updated_df[column_names['mp_on_market']]) & \
               (updated_df[column_names['mp_on_market']] > updated_df[column_names['stop']]) & \
               (~empty_stop_mask)# Исключаем строки с пустым 'stop'

        async def calculate_new_price(row):
            try:
                old_price = row[column_names['price']]
                mp_on_market = row[column_names['mp_on_market']]
                stop = row[column_names['stop']]
                shop_with_best_price = row[column_names['market_with_mp']]

                # Проверяем, является ли магазин с лучшей ценой одним из наших
                if shop_with_best_price in my_market:
                    return old_price, f"Цена не изменена. У одного из ваших магазинов ({shop_with_best_price}) уже минимальная цена на рынке.", None

                min_new_price = max(mp_on_market - 200, stop)
                max_new_price = mp_on_market - 50

                if min_new_price > max_new_price:
                    return old_price, f"Цена не изменена. Текущая цена: {old_price:.2f}, mp_on_market: {mp_on_market:.2f}, stop: {stop:.2f}", None

                new_price = max(random.randint(int(min_new_price), int(max_new_price)), int(stop))

                # Расчет новой discount_base только если цена изменилась
                new_discount_base = round(random.uniform(new_price * 1.3, new_price * 1.6), 0)

                return new_price, f"Цена изменена с {old_price:.2f} на {new_price:.2f}. Новая discount_base: {new_discount_base:.2f} (mp_on_market: {mp_on_market:.2f})", new_discount_base
            except Exception as e:
                logger.error(f"Ошибка при расчете новой цены для товара {row[column_names['seller_id']]}: {str(e)}")
                return row[column_names['price']], f"Ошибка при расчете новой цены: {str(e)}", None

        results = await asyncio.gather(*[calculate_new_price(row) for _, row in updated_df[mask].iterrows()])

        price_changed = pd.Series(False, index=updated_df.index)

        if results:
            new_prices, new_prims, new_discount_bases = zip(*results)
            price_changed[mask] = [new_price != old_price for new_price, old_price in
                                   zip(new_prices, updated_df.loc[mask, column_names['price']])]

            updated_df.loc[mask, column_names['price']] = new_prices
            updated_df.loc[mask, column_names['prim']] = new_prims

            # Обновляем discount_base только для товаров с измененной ценой
            discount_base_mask = mask & price_changed
            updated_df.loc[discount_base_mask, 'discount_base'] = [db for db, changed in
                                                                   zip(new_discount_bases, price_changed[mask]) if changed and db is not None]
        else:
            logger.info("Нет строк для обновления цен")

        for index, row in updated_df.iterrows():
            if not pd.isna(row[column_names['mp_on_market']]) and not pd.isna(row[column_names['stop']]):
                if row[column_names['mp_on_market']] <= row[column_names['stop']]:
                    warning_msg = f"Оптимальная цена mp_on_market ({row[column_names['mp_on_market']]:.2f}) ниже или равна минимальной stop ({row[column_names['stop']]:.2f}) для товара с артикулом {row[column_names['seller_id']]}"
                    logger.warning(warning_msg)
                    updated_df.loc[index, column_names['prim']] = warning_msg

        # Создаем for_update только для товаров с измененными ценами
        for_update = updated_df[price_changed].copy()
        try:
            updated_df = updated_df.drop('discount_base',axis=1)
        except :
            logger.warning("Колонка 'discount_base' отсутствует в DataFrame updated_df")
        # Убедимся, что колонка 'discount_base' присутствует в for_update
        if 'discount_base' not in for_update.columns:
            logger.warning("Колонка 'discount_base' отсутствует в DataFrame for_update")
            # Если колонки нет, добавим ее с пустыми значениями
            # for_update['discount_base'] = pd.NA

        return updated_df, for_update

    except Exception as e:
        logger.error(f"Критическая ошибка при обработке данных: {str(e)}")
        raise


# async def main():
#     # Определение названий колонок
#     column_names = {
#         'seller_id': 'SHOP_SKU',
#         'name': 'OFFER',
#         'link': 'LINK',
#         'price': 'MERCH_PRICE_WITH_PROMOS',
#         'stop': 'STOP',
#         'mp_on_market': 'PRICE.1',
#         'market_with_mp': 'SHOP_WITH_BEST_PRICE_ON_MARKET',
#         'prim': 'PRIM'
#     }
#
#     # Пример использования:
#     df1 = pd.read_csv('report_old.csv')
#     df1['LINK'] =''
#     df2 = pd.read_csv('report_old — копия.csv')
#
#
#     # Обновляем df1 данными из df2
#     updated_df = await update_dataframe(df1, df2, column_names)
#     print("Обновленный DataFrame:")
#     print(updated_df)
#     print("\nТипы данных:")
#     print(updated_df.dtypes)
#
#     # Создаем DataFrame for_update и логируем случаи, когда mp_on_market ниже stop
#     updated_df, for_update_df = await compare_prices_and_create_for_update(updated_df, column_names)
#     for_update_df.to_csv('for_updated.csv')
#     updated_df.to_csv('updated.csv')
#     print("\nDataFrame for_update:")
#     print(for_update_df)
#     print(updated_df)
#
#     await run_in_executor(for_update_df.to_csv, 'report/reported2.txt')
#
#     df = await run_in_executor(pd.read_csv, 'report/reported22.txt')
#     print(df.info())


# if __name__ == "__main__":
#     asyncio.run(main())
