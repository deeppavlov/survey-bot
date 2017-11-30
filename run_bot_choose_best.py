import csv
import random
from datetime import datetime
from itertools import chain
from typing import List, Tuple, Dict, Any
import re
import uuid

import os
from collections import namedtuple, defaultdict

import pickle
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler

INPUT_FILE = 'downloads/test_predict_243k_balanced_2911_0.csv'
CACHE_FILE = INPUT_FILE + '_pickbest.pickle'
OUTPUT_FILE = 'target/test_predict_243k_balanced_2911_0_{}.tsv'.format(datetime.now().strftime('%Y%m%dT%H%M%S'))
TOKEN = os.environ['SENSE_BOT_TOKEN']
OPERATOR_RANDOM = 'random'
OPERATOR_HUMAN = 'human'
OPERATOR_BOT = 'bot'
OPERATOR_BOT_BEST = 'botbest'
OPERATORS = [OPERATOR_BOT, OPERATOR_BOT_BEST]

Row = namedtuple('Row', 'id context question answer operator discriminator')


def prepare_dataset(filename=INPUT_FILE) -> Dict[str, List[Row]]:
    contexts = defaultdict(list)
    with open(filename) as f:
        csvfile = csv.reader(f, delimiter=',')
        next(csvfile)
        index = 0
        while True:
            try:
                text, is_human, discriminator_score = next(csvfile)
                context, *_ = text.split(' <ANS_START> ')
                chunks = re.findall(r'(<[A-Z_]+> [^<>]*)', text)
                answer = chunks[-1]

                assert answer.startswith('<ANS_START> '), text
                answer = answer.replace('<ANS_START> ', '')

                if chunks[-2].startswith('<MAN_START> '):
                    continue
                if chunks[-2].startswith('<PAUSE> '):
                    continue

                assert chunks[-2].startswith('<COR_START> '), [chunks[-2], text]
                question = chunks[-2].replace('<COR_START> ', '')

                if ('здравствуйте' in answer.lower()) and ('cлужба технической поддержки' in answer.lower()):
                    print(answer)
                    continue

                if int(is_human):
                    continue

                row = Row(index, context, question, answer, OPERATOR_BOT, float(discriminator_score))

                contexts[context].append(row)
                index += 1
            except StopIteration:
                break
            except IndexError:
                pass
    return contexts


def get_best_and_random_answer(dataset):
    for context, rows in dataset.items():
        rows = list(rows)
        if len(rows) == 1:
            continue

        best_row = max(rows, key=lambda x: x.discriminator)
        values = dict(zip(Row._fields, best_row))
        values['operator'] = OPERATOR_BOT_BEST
        best_row = Row(**values)

        random_row = random.choice(rows)

        if best_row.discriminator > random_row.discriminator:
            yield best_row, random_row


def shuffle(dataset):
    dataset = list(dataset)
    random.shuffle(dataset)
    return dataset


def prepare_message(message_store: Dict[str, Any], instance: Tuple[int, Tuple[Row, Row]]):
    questions_asked, [best_row, random_row] = instance
    answers = [best_row, random_row]
    order = [0, 1]
    random.shuffle(order)
    message = "{}\n\n<b>Ответ А:</b>\n{}\n<b>Ответ Б:</b>\n{}\n\nКакой ответ осмысленнее?".format(best_row.question,
                                                                  answers[order[0]].answer,
                                                                  answers[order[1]].answer)

    time_asked = datetime.now().isoformat()

    uid = uuid.uuid1().hex

    message_store[uid] = {'best': best_row, 'random': random_row, 'time_asked': time_asked}

    order = ['best' if e == 0 else 'random' for e in order]

    button_list = [
        [InlineKeyboardButton('Ответ А', callback_data='{};{}'.format(uid, order[0])),
         InlineKeyboardButton('Ответ Б', callback_data='{};{}'.format(uid, order[1]))],
        [InlineKeyboardButton('Нет разницы', callback_data='{};equal'.format(uid))]

    ]
    reply_markup = InlineKeyboardMarkup(button_list)

    return questions_asked, message, reply_markup


def main():
    updater = Updater(token=TOKEN)
    dispatcher = updater.dispatcher

    dialogs = {}

    exists = os.path.isfile(OUTPUT_FILE)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    messages_store = {}

    if not os.path.isfile(CACHE_FILE):
        print('Creating cache file {} ...'.format(CACHE_FILE))
        dataset = shuffle(get_best_and_random_answer(prepare_dataset(INPUT_FILE)))
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(dataset, f)
        print('Created!')

    with open(CACHE_FILE, 'rb') as f:
        dataset = pickle.load(f)

    with open(OUTPUT_FILE, 'a', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')

        if not exists:
            writer.writerow(['chat_id', 'user', 'result', 'question', 'best_answer', 'random_answer', 'context',
                             'best_discriminator', 'random_discriminator', 'time_asked', 'time_answered'])
            tsvfile.flush()

        def start(bot: Bot, update: Update):
            chat_id = update.message.chat_id
            dataset_rows = list(dataset)
            random.shuffle(dataset_rows)
            dialogs[chat_id] = {
                'batch_generator': iter(enumerate(dataset_rows))
            }

            startup_message = '''Добрый день, Толокер!

Каждые 10 фрагментов, система будет выводить количество оценённых ответов.
'''

            bot.send_message(chat_id=chat_id, text=startup_message)

            i, message, reply_markup = prepare_message(messages_store, next(dialogs[chat_id]['batch_generator']))
            bot.send_message(chat_id=chat_id, text=message,
                             reply_markup=reply_markup, parse_mode='HTML')

        def reply(bot: Bot, update: Update):
            query = update.callback_query
            chat_id = query.message.chat_id
            user = (update.effective_user.first_name or '') + '@' + (update.effective_user.username or '')

            uid, result = query.data.split(';')
            if uid in messages_store:
                best_row = messages_store[uid]['best']
                random_row = messages_store[uid]['random']
                time_asked = messages_store[uid]['time_asked']

                writer.writerow([chat_id, user, result, best_row.question, best_row.answer, random_row.answer,
                                 best_row.context, best_row.discriminator, random_row.discriminator,
                                 time_asked, datetime.now().isoformat()])
                tsvfile.flush()

            if chat_id not in dialogs:
                start(bot, query)
            else:
                i, message, reply_markup = prepare_message(messages_store, next(dialogs[chat_id]['batch_generator']))
                if i > 0 and i % 10 == 0:
                    bot.send_message(chat_id=chat_id, text='<i>Вы ответили на {} вопросов</i>'.format(i),
                                     parse_mode='HTML')
                bot.send_message(chat_id=chat_id, text=message,
                                 reply_markup=reply_markup, parse_mode='HTML')

        dispatcher.add_handler(CommandHandler('start', start))
        dispatcher.add_handler(CallbackQueryHandler(reply))

        updater.start_polling()

        updater.idle()


if __name__ == '__main__':
    main()
