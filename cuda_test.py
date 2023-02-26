import binance_data
from datetime import datetime
import postgreSQL_data

symbols = dict()  # symbol. Так проще создать уникальные символы
trade_list = list()  # start, symbol_first, side_first, symbol_second, side_second, symbol_third, side_third


def get_pair_list():
    # Запросим у бинанса какие торговые пары есть у биржы.
    pair_list = binance_data.get_all_rules()['symbols']
    return pair_list


def make_trade_list(sy, pair_list):
    isi = list()
    for row in pair_list:
        if not row['status'] == 'TRADING' or \
                "SPOT" not in row['permissions'] or \
                "LIMIT" not in row['orderTypes'] or \
                'LIMIT_MAKER' not in row['orderTypes'] or \
                'MARKET' not in row['orderTypes'] or \
                not row['quoteOrderQtyMarketAllowed']:
            continue

        if row['baseAsset'] == sy or row['quoteAsset'] == sy:
            side = 'BUY' if not row['baseAsset'] == sy else 'SELL'
            s = row['baseAsset'] if not row['baseAsset'] == sy else row['quoteAsset']
            isi.append([row, s, side])
    return isi


def make_tree_list(pair_list):
    print("{dt}: Загрузим торговые пары... ".format(dt=datetime.now().strftime("%Y-%m-%d %H-%M-%S-%f")))
    for ii, row in enumerate(pair_list, 0):

        if not row['status'] == 'TRADING' or \
                "SPOT" not in row['permissions'] or \
                "LIMIT" not in row['orderTypes'] or \
                'LIMIT_MAKER' not in row['orderTypes'] or \
                'MARKET' not in row['orderTypes'] or \
                not row['quoteOrderQtyMarketAllowed']:
            continue

        if symbols.get(row['baseAsset']) is None:
            symbols[row['baseAsset']] = [0, 0]
        if symbols.get(row['quoteAsset']) is None:
            symbols[row['quoteAsset']] = [0, 0]

    print("{dt}: Загрузка торговых пар выполнена... ".format(dt=datetime.now().strftime("%Y-%m-%d %H-%M-%S-%f")))


if __name__ == '__main__':
    # Создадим (очистим) таблицу в постгре, где будут храниться торговый протокол
    print(postgreSQL_data.createTable())

    all_pair_list = get_pair_list()
    print(f'Загрузим торговые пары: {len(all_pair_list)}')
    make_tree_list(all_pair_list)
    # Сложная шаманская ботва, которая выдает нужный результат.
    for ii, sym in enumerate(symbols, 1):
        cnt = 1
        isi1 = make_trade_list(sym, all_pair_list)
        for i1 in isi1:
            sym2 = i1[0]['baseAsset'] if not sym == i1[0]['baseAsset'] else i1[0]['quoteAsset']
            isi2 = make_trade_list(sym2, all_pair_list)
            for i2 in isi2:
                sym3 = i2[0]['baseAsset'] if not sym2 == i2[0]['baseAsset'] else i2[0]['quoteAsset']
                isi3 = make_trade_list(sym3, all_pair_list)
                for i3 in isi3:
                    if sym == i3[1]:
                        print(ii, ' - ', cnt, sym,
                              i1[0]['symbol'], i1[2], i1[1],
                              i2[0]['symbol'], i2[2], i2[1],
                              i3[0]['symbol'], i3[2], i3[1])
                        # Запишеи найденный протокол торговли в базу postgre
                        postgreSQL_data.InsertInto_tree_team(
                            [sym, i1[0]['symbol'], i1[2], i2[0]['symbol'], i2[2], i3[0]['symbol'], i3[2]])
                        cnt = cnt + 1
