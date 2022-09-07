import asyncio
import requests
import json
from aiogram import Bot, Dispatcher, executor, types
import sqlite3
import aioschedule
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.contrib.middlewares.logging import LoggingMiddleware

import keyboards
from utils import TestStates
from messages import MESSAGES

telegram_bot_api_token = '5431796869:AAHeZOj_VCULSCcYADV0-hPICGwjlfj198o'
apiUrl = "https://api.monday.com/v2"
bot = Bot(token=telegram_bot_api_token)
dp = Dispatcher(bot, storage=MemoryStorage())
dp.middleware.setup(LoggingMiddleware())
connection = sqlite3.connect("MondayBotDB.db")
cursor = connection.cursor()
cursor.execute("CREATE TABLE if not exists users (userid INTEGER, username TEXT)")
cursor.execute("CREATE TABLE if not exists projects (project_name TEXT, project_token TEXT, list_of_users TEXT)")


@dp.message_handler(commands=['start'])
async def start(message: types.Message):
    if len(cursor.execute(f'''SELECT userid FROM users WHERE userid = {message.from_user.id}''').fetchall()) == 0:
        state = dp.current_state(user=message.from_user.id)
        await state.set_state(TestStates.all()[1])
        await message.reply(MESSAGES["unregistered starter message"])
    else:
        await message.reply(MESSAGES["registered starter message"], reply_markup=keyboards.inline_kb1)


@dp.message_handler(commands=['create_new_project'])
async def create_new_project(message: types.Message):
    await message.reply(MESSAGES["set new projectname"])
    state = dp.current_state(user=message.from_user.id)
    await state.set_state(TestStates.all()[4])


@dp.message_handler(commands=['add_project'])
async def create_new_project(message: types.Message):
    await message.reply(MESSAGES["add project"])
    state = dp.current_state(user=message.from_user.id)
    await state.set_state(TestStates.all()[2])


@dp.message_handler(commands=['remove_project'])
async def remove_project(message: types.Message):
    await message.reply(MESSAGES["remove project"])
    state = dp.current_state(user=message.from_user.id)
    await state.set_state(TestStates.all()[3])


@dp.message_handler(commands=['select_project_change_status'])
async def start(message: types.Message):
    await message.reply(MESSAGES["select project change status"])
    state = dp.current_state(user=message.from_user.id)
    await state.set_state(TestStates.all()[6])


@dp.callback_query_handler(lambda c: c.data == 'button1')
async def process_callback_button1(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, 'Нажата первая кнопка!')


@dp.message_handler(state=TestStates.TEST_STATE_1)
async def first_test_state_case_met(message: types.Message):
    cursor.execute(f'''INSERT INTO users VALUES ({message.from_user.id}, '{message.text}')''')
    connection.commit()
    state = dp.current_state(user=message.from_user.id)
    await state.reset_state()
    await message.reply('Пользовательское имя успешно сохранено', reply=False)


@dp.message_handler(state=TestStates.TEST_STATE_2)
async def second_test_state_case_met(message: types.Message):
    list_of_users = cursor.execute(f'''SELECT list_of_users FROM projects WHERE project_name = \'{message.text}\'''').fetchone()[0]
    index = cursor.execute(f'''SELECT rowid FROM users WHERE userid = \'{message.from_user.id}\'''').fetchone()[0]
    cursor.execute(f'''UPDATE projects SET list_of_users = \'{list_of_users + ' ' + str(index)}\' WHERE project_name = \'{message.text}\'''')
    connection.commit()
    state = dp.current_state(user=message.from_user.id)
    await state.reset_state()
    await message.reply('Проект успешно добавлен', reply=False)


@dp.message_handler(state=TestStates.TEST_STATE_3)
async def third_test_state_case_met(message: types.Message):
    list_of_users = cursor.execute(f'''SELECT list_of_users FROM projects WHERE project_name = \'{message.text}\'''').fetchone()[0]
    index = cursor.execute(f'''SELECT rowid FROM users WHERE userid = \'{message.from_user.id}\'''').fetchone()[0]
    list_of_users = list_of_users.split()
    list_of_users.remove(str(index))
    cursor.execute(f'''UPDATE projects SET list_of_users = '{' '.join(list_of_users)}' WHERE project_name = \'{message.text}\'''')
    connection.commit()
    state = dp.current_state(user=message.from_user.id)
    await state.reset_state()
    await message.reply('Проект успешно удален из вашего списка', reply=False)


@dp.message_handler(state=TestStates.TEST_STATE_4)
async def fourth_test_state_case_met(message: types.Message):
    cursor.execute(f'''INSERT INTO projects VALUES ('{message.text}', '{message.from_user.id}', '')''')
    connection.commit()
    state = dp.current_state(user=message.from_user.id)
    await state.set_state(TestStates.all()[5])
    await message.reply(MESSAGES['set project token'], reply=False)


@dp.message_handler(state=TestStates.TEST_STATE_5)
async def fifth_test_state_case_met(message: types.Message):
    cursor.execute(f'''UPDATE projects SET project_token = '{message.text}' WHERE project_token = {message.from_user.id}''')
    connection.commit()
    state = dp.current_state(user=message.from_user.id)
    await state.reset_state()
    await message.reply('Новый проект успешно создан', reply=False)


@dp.message_handler(state=TestStates.TEST_STATE_6)
async def sixth_test_state_case_met(message: types.Message, state: FSMContext):
    await message.reply(MESSAGES["select task change status"])
    stt = dp.current_state(user=message.from_user.id)
    async with state.proxy() as data:
        data['project name'] = message.text
    await stt.set_state(TestStates.all()[7])


@dp.message_handler(state=TestStates.TEST_STATE_7)
async def seventh_test_state_case_met(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        project_name = data['project name']
    project = cursor.execute(f'''SELECT * FROM projects WHERE project_name = "{project_name}"''').fetchone()
    monday_api_key = project[1]
    headers = {"Authorization": monday_api_key}
    query = '{boards(limit:1) { name id description items { name column_values{title id type text } } } }'
    data = {'query': query}
    r = requests.post(url=apiUrl, json=data, headers=headers)
    dct = json.loads(r.text)
    username = cursor.execute(f'''SELECT username FROM users WHERE userid = {message.from_user.id}''').fetchone()[0]
    for task in dct["data"]["boards"][0]["items"]:
        if task["name"] == message.text:
            if username in task["column_values"][0]["text"]:
                query2 = '''
                query{
                  boards(ids:3027808493){
                    items{
                      id
                      name
                    }
                  }
                }
                '''
                data2 = {'query': query2}
                r2 = requests.post(url=apiUrl, json=data2, headers=headers)
                dct2 = json.loads(r2.text)
                for innertask in dct2["data"]["boards"][0]["items"]:
                    if innertask["name"] == message.text:
                        task_id = innertask["id"]
                        query3 = '''
                        mutation changeValues {
                            change_column_value (board_id: ''' + str(dct["data"]["boards"][0]["id"]) + ', item_id: ' + str(task_id) + ''' , column_id: "status", value: "{\\"index\\": 0}") {
                              id
                            }
                          }
                        '''
                        data3 = {'query': query3}
                        r3 = requests.post(url=apiUrl, json=data3, headers=headers)
    await state.finish()


async def shutdown(dispatcher: Dispatcher):
    await dispatcher.storage.close()
    await dispatcher.storage.wait_closed()


async def send_notifications():
    projects = cursor.execute("SELECT * FROM projects").fetchall()
    for project in projects:
        monday_api_key = project[1]
        headers = {"Authorization": monday_api_key}
        query = '{boards(limit:1) { name id description items { name column_values{title id type text } } } }'
        data = {'query': query}
        r = requests.post(url=apiUrl, json=data, headers=headers)
        dct = json.loads(r.text)
        for item in dct["data"]["boards"][0]["items"]:
            if item["column_values"][1]["text"] != "Готово":
                uids = cursor.execute(f'''SELECT userid FROM users WHERE username = \'{item["column_values"][0]["text"]}\'''').fetchall()
                if uids:
                    for uid in uids:
                        await bot.send_message(int(uid[0]), f'''{item["name"]} {item["column_values"][2]["text"]}''')


async def scheduler():
    aioschedule.at.do(send_notifications)
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)


async def on_startup(_):
    asyncio.create_task(scheduler())


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
