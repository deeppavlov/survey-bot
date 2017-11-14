import configparser
import csv
import random

import time

import os
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler

import html


# def prepare_dataset(config):
#     questions = open(config['questions_file']).readlines()
#     live_answers = open(config['live_answers_file']).readlines()
#     bot_answers = open(config['bot_answers_file']).readlines()
#
#     questions = [(i, "<b>Клиент:</b> {}".format(html.escape(q.split('>')[-1].strip())))
#                  for i, q in enumerate(questions)]
#     live_answers = [(0, "<b>Оператор:</b> {}".format(html.escape(a.strip()))) for a in live_answers]
#     bot_answers = [(1, "<b>Оператор:</b> {}".format(html.escape(a.strip()))) for a in bot_answers]
#
#     data = list(zip(
#         questions,
#         live_answers,
#         bot_answers
#     ))
#     return data[:int(config['end_on'])]


def prepare_dataset(config):
    data = []
    with open('downloads/predicted_Denis.csv') as f:
        csvfile = csv.reader(f, delimiter=',')
        header = f.readline().strip().split(',')
        index = 0
        while True:
            try:
                answer_human, is_human1, human_score = next(csvfile)
                answer_bot, is_human2, bot_score = next(csvfile)

                assert is_human1 == '1'
                assert is_human2 == '0'

                question, human = answer_human.split('<ANS_START>')
                question2, bot = answer_bot.split('<ANS_START>')

                assert question == question2

                data.append(((index,
                              float(human_score),
                              float(bot_score),
                              "{}\n".format(html.escape(question.split('>')[-1].strip()))),
                            (0, html.escape(human.strip())),
                            (1, html.escape(bot.strip()))))
                index += 1
            except StopIteration:
                break
    return data


def batch_generator_generator(data):
    def batch_generator():
        questions_asked = 0
        while True:
            seq = list(data)
            random.shuffle(seq)
            while seq:
                q_l = []
                q_b = []
                for _ in range(5):
                    q = seq.pop()
                    q_l.append((q[0], q[1]))
                    q_b.append((q[0], q[2]))
                random.shuffle(q_l)
                random.shuffle(q_b)
                q = [q_l, q_b]
                random.shuffle(q)
                q = [j for i in zip(q[0], q[1]) for j in i]
                while q:
                    yield questions_asked, q.pop()
                    questions_asked += 1

    return batch_generator()


def prepare_message(instance):
    questions_asked, ((q_id, human_score, bot_score, question), (bot_or_not, answer)) = instance
    message = question + '\n' + "<b>Продолжение:</b>\n{}".format(answer)

    t = int(time.time())

    button_list = [
        [InlineKeyboardButton('Осмысленно', callback_data='{}:{}:{}:{}:{}:1'.format(t, q_id, human_score, bot_score, bot_or_not)),
         InlineKeyboardButton('Не осмысленно', callback_data='{}:{}:{}:{}:{}:0'.format(t, q_id, human_score, bot_score, bot_or_not))]
    ]
    reply_markup = InlineKeyboardMarkup(button_list)

    return questions_asked, message, reply_markup


def main():
    config = configparser.ConfigParser()
    config.read('bot.ini')
    config = config['DEFAULT']

    token = config['bot_token']
    data = prepare_dataset(config)

    updater = Updater(token=token)
    dispatcher = updater.dispatcher

    dialogs = {}

    out_file_path = config['out_tsv']
    exists = os.path.isfile(out_file_path)
    os.makedirs(os.path.dirname(out_file_path), exist_ok=True)

    with open(out_file_path, 'a', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')

        if not exists:
            writer.writerow(['chat_id', 'question_id', 'question', 'is_bot', 'answer', 'time_asked', 'time_answered',
                             'is_meaningful', 'human_score', 'bot_score'])
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

            time_asked, question_id, human_score, bot_score, is_bot, is_meaningful = query.data.split(':')
            question_id = int(question_id)
            is_bot = int(is_bot)
            writer.writerow([chat_id, question_id, data[question_id][0][3], is_bot, data[question_id][is_bot+1][1],
                             time_asked, int(time.time()), is_meaningful, human_score, bot_score])
            tsvfile.flush()

            if chat_id not in dialogs:
                start(bot, query)
            else:
                i, message, reply_markup = prepare_message(next(dialogs[chat_id]['batch_generator']))
                if (i + 1) % 10 == 0:
                    bot.send_message(chat_id=chat_id, text='<i>отвечено на {} вопросов</i>'.format(i+1),
                                     parse_mode='HTML')
                bot.send_message(chat_id=chat_id, text=message,
                                 reply_markup=reply_markup, parse_mode='HTML')

        dispatcher.add_handler(CommandHandler('start', start))
        dispatcher.add_handler(CallbackQueryHandler(reply))

        updater.start_polling()

        updater.idle()


if __name__ == '__main__':
    main()
