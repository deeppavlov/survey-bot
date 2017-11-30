import csv
import random
from datetime import datetime
from itertools import groupby, chain
from typing import List, Tuple, Iterator
import re

import os
from collections import namedtuple

import pickle
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler


INPUT_FILE = 'downloads/sber3.csv'
OUTPUT_FILE = 'target/sber3_{}.tsv'.format(datetime.now().strftime('%Y%m%dT%H%M%S'))
TOKEN = os.environ['SENSE_BOT_TOKEN']
OPERATOR_RANDOM = 'random'
OPERATOR_HUMAN = 'human'
OPERATOR_BOT = 'bot'

Row = namedtuple('Row', 'id question answer operator discriminator')


def prepare_dataset(filename=INPUT_FILE) -> Iterator[Row]:
    cache = {}
    with open(filename) as f:
        csvfile = csv.reader(f, delimiter=',')
        next(csvfile)
        index = 0
        while True:
            try:
                is_human, text, discriminator_score = next(csvfile)
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

                # if len(set(word_tokenize(question))) < 7:
                #     continue

                row = Row(index, question, answer, OPERATOR_HUMAN if int(is_human) else OPERATOR_BOT,
                          discriminator_score)

                # if len(set(answer.split())) > 15:
                #     continue

                # if row.operator == OPERATOR_BOT and float(row.discriminator) < 0.5:
                #     continue

                # if row.operator != OPERATOR_BOT and random.randint(1, 2) == 1:
                #     continue

                yield row
                index += 1
            except StopIteration:
                break
            except IndexError:
                pass


def mixin_random_answers(dataset):
    dataset = list(dataset)
    answers = [d.answer for d in dataset]
    random.shuffle(answers)

    human_count = 0
    bot_count = 0
    random_count = 0

    # for the 1/3 of data corrupt answer with random answer from the whole dataset
    for d, random_answer in zip(dataset, answers):
        if random.randint(1, 3) == 3:
            yield Row(d.id, d.question, random_answer, OPERATOR_RANDOM, None)
            random_count += 1
        else:
            yield d
            if d.operator == OPERATOR_HUMAN:
                human_count += 1
            elif d.operator == OPERATOR_BOT:
                bot_count += 1
            else:
                raise Exception('Unknown operator {}'.format(d.operator))


def filter_duplicate_answers(dataset):
    by_question = lambda row: row.question

    for group, diff_answers in groupby(sorted(dataset, key=by_question), key=by_question):
        data = list(diff_answers)
        bots = [row for row in data if row.operator == 'bot']
        if bots:
            max_score_row = max(bots, key=lambda x: float(x.discriminator))
            min_score_row = min(bots, key=lambda x: float(x.discriminator))

            yield max_score_row
            if len(bots) > 1:
                yield min_score_row
            yield from (row for row in data if row.operator != 'bot')


def balance_operators(dataset):
    dataset = list(dataset)
    operators = [OPERATOR_HUMAN, OPERATOR_BOT, OPERATOR_RANDOM]
    operatos_rows = [[r for r in dataset if r.operator == op] for op in operators]
    dataset = list(chain(*zip(*operatos_rows)))
    random.shuffle(dataset)
    return dataset


def numerate_ids(dataset):
    dataset = list(dataset)
    for op in [OPERATOR_HUMAN, OPERATOR_BOT, OPERATOR_RANDOM]:
        print(op, ':', len([1 for r in dataset if r.operator == op]))
    return {r.id: r for r in dataset}


def batch_generator_generator(data):
    seq = list(data)

    def batch_generator():
        questions_asked = 0
        while True:
            random.shuffle(seq)
            for q in seq:
                yield questions_asked, q
                questions_asked += 1
    return batch_generator()


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

    cached_file = INPUT_FILE + '.pickle'
    if not os.path.isfile(cached_file):
        print('Creating cache file {} ...'.format(cached_file))
        dataset = numerate_ids(balance_operators(mixin_random_answers(filter_duplicate_answers(prepare_dataset(INPUT_FILE)))))
        with open(cached_file, 'wb') as f:
            pickle.dump(dataset, f)
        print('Created!')

    with open(cached_file, 'rb') as f:
        dataset = pickle.load(f)

    with open(OUTPUT_FILE, 'a', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')

        if not exists:
            writer.writerow(['chat_id', 'user', 'question_id', 'operator', 'question', 'answer', 'time_asked', 'time_answered',
                             'is_meaningful', 'discriminator_score'])
            tsvfile.flush()

        def start(bot: Bot, update: Update):
            chat_id = update.message.chat_id
            dialogs[chat_id] = {
                'batch_generator': batch_generator_generator(dataset)
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
            question = dataset[question_id][1]
            answer = dataset[question_id][2]
            writer.writerow([chat_id, user, question_id, operator, question, answer,
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
