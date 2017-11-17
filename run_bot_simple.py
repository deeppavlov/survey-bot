import csv
import random
from datetime import datetime
from typing import List, Tuple
import time

import os
from collections import namedtuple

from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler

import html


OUTPUT_FILE = 'target/sber3_w_random.tsv'
TOKEN = os.environ['SENSE_BOT_TOKEN']
OPERATOR_RANDOM = 'random'
OPERATOR_HUMAN = 'human'
OPERATOR_BOT = 'bot'

Row = namedtuple('Row', 'id question answer operator discriminator')


def prepare_dataset():
    data = []  # type: List[Row]
    with open('downloads/sber2.csv') as f:
        csvfile = csv.reader(f, delimiter=',')
        next(csvfile)
        index = 0
        while True:
            try:
                is_human, text, discriminator_score = next(csvfile)
                context, answer = text.strip().split('<ANS_START>')

                cs = context.replace('<COR_START>', ';').replace('<MAN_START>', ';').replace('<PAUSE>', ';')
                cs = [c.strip() for c in cs.split(';') if c.strip()]

                question = cs[-1]

                data.append(Row(index, question, answer,
                                OPERATOR_HUMAN if int(is_human) else OPERATOR_BOT, discriminator_score))
                index += 1
            except StopIteration:
                break
            except IndexError:
                pass

    answers = [d[2] for d in data]
    random.shuffle(answers)

    res = []
    # for the 1/3 of data corrupt answer with random answer from the whole dataset
    for d, random_answer in zip(data, answers):
        if random.randint(1, 3) == 3:
            res.append(Row(d.id, d.question, random_answer, OPERATOR_RANDOM, None))
        else:
            res.append(d)

    return res


def batch_generator_generator(data):
    def batch_generator():
        questions_asked = 0
        while True:
            seq = list(data)
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
    data = prepare_dataset()

    updater = Updater(token=TOKEN)
    dispatcher = updater.dispatcher

    dialogs = {}

    exists = os.path.isfile(OUTPUT_FILE)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, 'a', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')

        if not exists:
            writer.writerow(['chat_id', 'user', 'question_id', 'operator', 'question', 'answer', 'time_asked', 'time_answered',
                             'is_meaningful', 'discriminator_score'])
            tsvfile.flush()

        def start(bot: Bot, update: Update):
            chat_id = update.message.chat_id
            dialogs[chat_id] = {
                'batch_generator': batch_generator_generator(data)
            }
            i, message, reply_markup = prepare_message(next(dialogs[chat_id]['batch_generator']))
            bot.send_message(chat_id=chat_id, text=message,
                             reply_markup=reply_markup, parse_mode='HTML')

        def reply(bot: Bot, update: Update):
            query = update.callback_query
            chat_id = query.message.chat_id
            user = (update.effective_user.first_name or '') + '@' + (update.effective_user.username or '')

            time_asked, question_id, score, operator, is_meaningful = query.data.split(';')
            question_id = int(question_id)
            question = data[question_id][1]
            answer = data[question_id][2]
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
