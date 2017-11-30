import csv
import random
from datetime import datetime
from itertools import chain
from typing import List, Tuple, Dict
import re

import os
from collections import namedtuple, defaultdict

import pickle
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler


INPUT_FILE = 'downloads/test_predict_243k_balanced_2911_0.csv'
CACHE_FILE = INPUT_FILE + '2var.pickle'
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

        yield best_row
        yield random_row


def balance_and_shuffle(dataset):
    dataset = list(dataset)
    operatos_rows = [[r for r in dataset if r.operator == op] for op in OPERATORS]
    dataset = list(chain(*zip(*operatos_rows)))
    random.shuffle(dataset)
    return dataset


def numerate_ids(dataset):
    dataset = list(dataset)
    for op in OPERATORS:
        print(op, ':', len([1 for r in dataset if r.operator == op]))
    return {r.id: r for r in dataset}


def prepare_message(instance: Tuple[int, Row]):
    questions_asked, row = instance
    message = row.question + '\n' + "<b>Ответ:</b>\n{}".format(row.answer)

    t = datetime.now().isoformat()

    button_list = [
        [InlineKeyboardButton('Осмысленно', callback_data='{0};{r.id};{r.discriminator};{r.operator};1'.format(t, r=row)),
         InlineKeyboardButton('Не осмысленно', callback_data='{0};{r.id};{r.discriminator};{r.operator};0'.format(t, r=row))]

    ]
    reply_markup = InlineKeyboardMarkup(button_list)

    return questions_asked, message, reply_markup


def main():
    updater = Updater(token=TOKEN)
    dispatcher = updater.dispatcher

    dialogs = {}

    exists = os.path.isfile(OUTPUT_FILE)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    if not os.path.isfile(CACHE_FILE):
        print('Creating cache file {} ...'.format(CACHE_FILE))
        dataset = numerate_ids(balance_and_shuffle(get_best_and_random_answer(prepare_dataset(INPUT_FILE))))
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(dataset, f)
        print('Created!')

    with open(CACHE_FILE, 'rb') as f:
        dataset = pickle.load(f)

    with open(OUTPUT_FILE, 'a', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')

        if not exists:
            writer.writerow(['chat_id', 'user', 'question_id', 'context', 'operator', 'question', 'answer',
                             'time_asked', 'time_answered', 'is_meaningful', 'discriminator_score'])
            tsvfile.flush()

        def start(bot: Bot, update: Update):
            chat_id = update.message.chat_id
            dataset_rows = list(dataset.values())
            random.shuffle(dataset_rows)
            dialogs[chat_id] = {
                'batch_generator': iter(enumerate(dataset_rows))
            }

            startup_message = '''Добрый день! Сейчас вам будут представлены фрагменты из чата оператора поддержки банка с клиентом. Просим вас оценить ответ оператора на вопрос клиента по степени осмысленности. Осмысленность понимайте как ваше субъективное ощущение того, что оператор понимает запрос клиента и пытается помочь.

Каждые 10 фрагментов, система будет выводить количество оценённых ответов.
'''

            bot.send_message(chat_id=chat_id, text=startup_message)

            i, message, reply_markup = prepare_message(next(dialogs[chat_id]['batch_generator']))
            bot.send_message(chat_id=chat_id, text=message,
                             reply_markup=reply_markup, parse_mode='HTML')

        def reply(bot: Bot, update: Update):
            query = update.callback_query
            chat_id = query.message.chat_id
            user = (update.effective_user.first_name or '') + '@' + (update.effective_user.username or '')

            time_asked, question_id, score, operator, is_meaningful = query.data.split(';')
            question_id = int(question_id)
            question = dataset[question_id].question
            answer = dataset[question_id].answer
            context = dataset[question_id].context
            writer.writerow([chat_id, user, question_id, context, operator, question, answer,
                             time_asked, datetime.now().isoformat(), is_meaningful, score])
            tsvfile.flush()

            if chat_id not in dialogs:
                start(bot, query)
            else:
                i, message, reply_markup = prepare_message(next(dialogs[chat_id]['batch_generator']))
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
