import configparser
import random

from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler


def prepare_dataset(config):
    questions = open(config['questions_file']).readlines()
    questions = [q.split('>')[-1] for q in questions]
    data = list(zip(
        questions,
        open(config['live_answers_file']).readlines(),
        open(config['bot_answers_file']).readlines()
    ))
    return data[int(config['start_from']):int(config['end_on'])]


def prepare_message(question, answer):
    return str(question) + '\n' + str(answer)


def batch_generator_generator(data):
    def batch_generator():
        while True:
            seq = list(data)
            random.shuffle(seq)
            while seq:
                q_l = []
                q_b = []
                for _ in range(5):
                    q = seq.pop()
                    q_l.append(prepare_message(q[0], q[1]))
                    q_b.append(prepare_message(q[0], q[2]))
                random.shuffle(q_l)
                random.shuffle(q_b)
                q = [q_l, q_b]
                random.shuffle(q)
                q = [j for i in zip(q[0], q[1]) for j in i]
                while q:
                    yield q.pop()
    return batch_generator()


def main():
    config = configparser.ConfigParser()
    config.read('bot.ini')
    config = config['DEFAULT']

    token = config['bot_token']
    data = prepare_dataset(config)

    updater = Updater(token=token)
    dispatcher = updater.dispatcher

    dialogs = {}

    button_list = [
        [InlineKeyboardButton('Осмысленно', callback_data='1'),
         InlineKeyboardButton('Не осмысленно', callback_data='0')]
    ]
    reply_markup = InlineKeyboardMarkup(button_list)

    def start(bot: Bot, update: Update):
        chat_id = update.message.chat_id
        dialogs[chat_id] = {
            'batch_generator': batch_generator_generator(data)
        }
        bot.send_message(chat_id=chat_id, text=next(dialogs[chat_id]['batch_generator']), reply_markup=reply_markup)

    def reply(bot: Bot, update: Update):
        query = update.callback_query
        chat_id = query.message.chat_id
        if chat_id not in dialogs:
            start(bot, query)
        else:
            bot.send_message(chat_id=chat_id, text=next(dialogs[chat_id]['batch_generator']), reply_markup=reply_markup)

    dispatcher.add_handler(CommandHandler('start', start))
    dispatcher.add_handler(CallbackQueryHandler(reply))

    updater.start_polling()

    updater.idle()


if __name__ == '__main__':
    main()
