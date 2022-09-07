from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

inline_btn_1 = InlineKeyboardButton('Мои задания', callback_data='button1')
inline_kb1 = InlineKeyboardMarkup().add(inline_btn_1)
