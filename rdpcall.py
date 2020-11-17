#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "SAI"
__license__ = "GPLv3"
__email__ = "andrew.foma@gmail.com"
__status__ = "Dev"

from aioconsole import ainput
from ipaddress import ip_address, ip_network
from collections import namedtuple
from aiofiles import open as aiofiles_open
from os import path
import ujson
import asyncio
import argparse
import datetime
import copy

from typing import (Any,
                    NamedTuple,
                    Iterator,
                    BinaryIO,
                    TextIO,
                    )


def dict_paths(some_dict: dict,
               path: set = ()):
    """
    Итератор по ключам в словаре
    :param some_dict:
    :param path:
    :return:
    """
    for key, value in some_dict.items():
        key_path = path + (key,)
        yield key_path
        if hasattr(value, 'items'):
            yield from dict_paths(value, key_path)


def check_path(some_dict: dict,
               path_sting: str) -> bool:
    """
    Проверяет наличие ключа
    :param some_dict:
    :param path_sting:
    :return:
    """
    if isinstance(some_dict, dict):
        all_paths = set(['.'.join(p) for p in dict_paths(some_dict)])
        if path_sting in all_paths:
            return True


def return_value_from_dict(some_dict: dict,
                           path_string: str) -> Any:
    """
    Возвращает значение ключа в словаре по пути ключа "key.subkey.subsubkey"
    :param some_dict:
    :param path_string:
    :return:
    """
    if check_path(some_dict, path_string):
        keys = path_string.split('.')
        _c = some_dict.copy()
        for k in keys:
            _c = _c[k]
        return _c


def check_ip(ip_str: str) -> bool:
    """
    Проверка строки на ip адрес
    :param ip_str:
    :return:
    """
    try:
        ip_address(ip_str)
        return True
    except BaseException:
        return False


def check_network(net_str: str) -> bool:
    """
    Проверка строки на ip адрес сети
    :param net_str:
    :return:
    """
    try:
        ip_network(net_str)
        return True
    except BaseException:
        return False


async def write_to_stdout(object_file: BinaryIO,
                          record_str: str):
    """
    write in 'wb' mode to object_file, input string in utf-8
    :param object_file:
    :param record_str:
    :return:
    """
    await object_file.write(record_str.encode('utf-8') + b'\n')


async def write_to_file(object_file: TextIO,
                        record_str: str):
    """
    write in 'text' mode to object_file
    :param object_file:
    :param record_str:
    :return:
    """
    await object_file.write(record_str + '\n')


def create_template_error(target: NamedTuple,
                          error_str: str) -> dict:
    """
    функция создает шаблон ошибочной записи(результата), добавляет строку error_str
    :param target:
    :param error_str:
    :return:
    """
    _tmp = {'ip': target.ip,
            'port': target.port,
            'data': {}}
    _tmp['data']['rdp'] = {'status': 'unknown-error',
                           'error': error_str}
    return _tmp


def create_target_rdp_protocol(ip_str: str,
                               settings: dict) -> Iterator:
    """
    На основании ip адреса и настроек возвращает через yield
    экзэмпляр namedtuple - Target.
    Каждый экземпляр Target содержит всю необходимую информацию(настройки и параметры) для функции worker.
    :param ip_str:
    :param settings:
    :return:
    """
    current_settings = copy.copy(settings)
    key_names = list(current_settings.keys())
    key_names.extend(['ip'])
    Target = namedtuple('Target', key_names)
    current_settings['ip'] = ip_str
    target = Target(**current_settings)
    yield target


def create_targets_rdp_protocol(ip_str: str,
                                settings: dict) -> Iterator[NamedTuple]:
    """
    Функция для обработки "подсетей" и создания "целей"
    :param ip_str:
    :param settings:
    :return:
    """
    hosts = ip_network(ip_str, strict=False)
    for host in hosts:
        for target in create_target_rdp_protocol(str(host), settings):
            yield target


async def read_input_file(queue_input: asyncio.Queue,
                          settings: dict,
                          path_to_file: str) -> None:
    """
    посредством модуля aiofiles функция "асинхронно" читает из файла записи, представляющие собой
    обязательно или ip адрес или запись подсети в ipv4
    из данной записи формируется экзэмпляр NamedTuple - Target, который отправляется в Очередь
    :param queue_results:
    :param settings:
    :param path_to_file:
    :return:
    """
    global count_input
    async with aiofiles_open(path_to_file, mode='rt') as f:  # read str
        async for line in f:
            linein = line.strip()
            if any([check_ip(linein), check_network(linein)]):
                targets = create_targets_rdp_protocol(linein, settings) # generator
                if targets:
                    for target in targets:
                        check_queue = True
                        while check_queue:
                            size_queue = queue_input.qsize()
                            if size_queue < queue_limit_targets-1:
                                count_input += 1  # statistics
                                queue_input.put_nowait(target)
                                check_queue = False
                            else:
                                await asyncio.sleep(sleep_duration_queue_full)
    await queue_input.put(b"check for end")


async def read_input_stdin(queue_input: asyncio.Queue,
                           settings: dict,
                           path_to_file: str = None) -> None:
    """
        посредством модуля aioconsole функция "асинхронно" читает из stdin записи, представляющие собой
        обязательно или ip адрес или запись подсети в ipv4
        из данной записи формируется экзэмпляр NamedTuple - Target, который отправляется в Очередь
        TODO: использовать один модуль - или aioconsole или aiofiles
        :param queue_results:
        :param settings:
        :param path_to_file:
        :return:
        """
    global count_input
    while True:
        try:
            _tmp_input = await ainput()  # read str from stdin
            linein = _tmp_input.strip()
            if any([check_ip(linein), check_network(linein)]):
                targets = create_targets_rdp_protocol(linein, settings)
                if targets:
                    for target in targets:
                        check_queue = True
                        while check_queue:
                            size_queue = queue_input.qsize()
                            if size_queue < queue_limit_targets - 1:
                                count_input += 1  # statistics
                                queue_input.put_nowait(target)
                                check_queue = False
                            else:
                                await asyncio.sleep(sleep_duration_queue_full)
        except EOFError:
            await queue_input.put(b"check for end")
            break


def checkfile(path_to_file: str) -> bool:
    return path.isfile(path_to_file)


async def run_external_screenshot(target: NamedTuple,
                        semaphore: asyncio.Semaphore,
                        queue_out: asyncio.Queue):
    """
    сопрограмма, осуществляет подключение к Target,
    отправку и прием данных, формирует результата в виде dict
    :param target:
    :param semaphore:
    :return:
    """
    global count_good
    global count_error
    async with semaphore:
        _target_script = f"--width {target.width} --height {target.height} --target {target.ip}:{target.port}"
        target_script = f"{template_run_script} {_target_script}"
        proc = await asyncio.create_subprocess_shell(target_script,
                                                     limit=1024 * 4096,  # 4 MiB
                                                     stdout=asyncio.subprocess.PIPE,
                                                     stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        result = None
        str_error = ''
        if stdout:
            line = None
            try:
                result = ujson.loads(stdout.decode())
            except:
                pass
            if result:
                success = return_value_from_dict(result, "data.rdp.status")
                if success == "success":
                    count_good += 1
                else:
                    count_error += 1
                try:
                    if args.show_only_success:
                        if success == "success":
                            line = ujson.dumps(result)
                    else:
                        line = ujson.dumps(result)
                except Exception as e:
                    pass
                if line:
                    await queue_out.put(line)
            else:
                try:
                    str_error = stdout.decode()
                    str_error = str_error + f"__returncode__:{proc.returncode}"
                except:
                    pass
        elif stderr:
            str_error = 'some errors'
            try:
                str_error = ujson.loads(stderr.decode())
            except:
                pass
        else:
            str_error = 'empty'
        if str_error:
            count_error += 1
            result_error: dict = create_template_error(target, str(str_error))
            line = ujson.dumps(result_error)
            await queue_out.put(line)


async def work_with_create_tasks_queue(queue_with_input: asyncio.Queue,
                                      queue_with_tasks: asyncio.Queue,
                                      queue_out: asyncio.Queue,
                                      count: int) -> None:
    """

    :param queue_with_input:
    :param queue_with_tasks:
    :param queue_out:
    :param count:
    :return:
    """
    semaphore = asyncio.Semaphore(count)
    while True:
        # wait for an item from the "start_application"
        item = await queue_with_input.get()
        if item == b"check for end":
            await queue_with_tasks.put(b"check for end")
            break
        if item:
            task = asyncio.create_task(
                run_external_screenshot(item, semaphore, queue_out))
            await queue_with_tasks.put(task)


async def work_with_queue_tasks(queue_results: asyncio.Queue,
                                queue_prints: asyncio.Queue) -> None:
    """

    :param queue_results:
    :param queue_prints:
    :return:
    """
    while True:
        # wait for an item from the "start_application"
        task = await queue_results.get()
        if task == b"check for end":
            await queue_prints.put(b"check for end")
            break
        if task:
            await task


async def work_with_queue_result(queue_out: asyncio.Queue,
                                 filename,
                                 mode_write) -> None:
    """

    :param queue_out:
    :param filename:
    :param mode_write:
    :return:
    """
    if mode_write == 'a':
        method_write_result = write_to_file
    else:
        method_write_result = write_to_stdout
    async with aiofiles_open(filename, mode=mode_write) as file_with_results:
        while True:
            line = await queue_out.get()
            if line == b"check for end":
                break
            if line:
                await method_write_result(file_with_results, line)
    await asyncio.sleep(0.5)
    # region dev
    if args.statistics:
        stop_time = datetime.datetime.now()
        _delta_time = stop_time - start_time
        duration_time_sec = _delta_time.total_seconds()
        statistics = {'duration': duration_time_sec,
                      'valid targets': count_input,
                      'success': count_good,
                      'fails': count_error}
        async with aiofiles_open('/dev/stdout', mode='wb') as stats:
            await stats.write(ujson.dumps(statistics).encode('utf-8') + b'\n')
    # endregion


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='RDPscreenshot executor lite(asyncio)')

    parser.add_argument(
        "-f",
        "--input-file",
        dest='input_file',
        type=str,
        help="path to file with targets")

    parser.add_argument(
        "-o",
        "--output-file",
        dest='output_file',
        type=str,
        help="path to file with results")

    parser.add_argument(
        "-s",
        "--senders",
        dest='senders',
        type=int,
        default=16,
        help="Number of coroutines to use (default: 16)")

    parser.add_argument(
        "--queue-sleep",
        dest='queue_sleep',
        type=int,
        default=1,
        help='Sleep duration if the queue is full, default 1 sec. Size queue == senders')


    parser.add_argument(
        "--width",
        dest='width',
        type=int,
        default=800,
        help="width")

    parser.add_argument(
        "--height",
        dest='height',
        type=int,
        default=600,
        help="height")

    parser.add_argument(
        "--timeout",
        dest='timeout',
        type=int,
        default=5,
        help="timeout")

    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=3389,
        help='Specify port (default: 3389)')

    parser.add_argument(
        '--show-only-success',
        dest='show_only_success',
        action='store_true')

    parser.add_argument(
        '--show-statistics',
        dest='statistics',
        action='store_true')

    path_to_file_targets = None  # set default None to inputfile
    args = parser.parse_args()

    # region parser ARGs

    # в method_create_targets - метод, которые или читает из stdin или из
    # файла
    if not args.input_file:
        # set method - async read from stdin (str)
        method_create_targets = read_input_stdin
    else:
        # set method - async read from file(txt, str)
        method_create_targets = read_input_file

        path_to_file_targets = args.input_file
        if not checkfile(path_to_file_targets):
            print(f'ERROR: file not found: {path_to_file_targets}')
            exit(1)

    if not args.output_file:
        output_file, mode_write = '/dev/stdout', 'wb'
    else:
        output_file, mode_write = args.output_file, 'a'

        # endregion
    settings = {"port": args.port,
                "timeout": args.timeout,
                "width": args.width,
                "height": args.height,
                "mode": "stdout"
                }
    path_to_rdpy = "/rdpyscreenshot"
    template_run_script = f"timeout {args.timeout+5} xvfb-run --auto-servernum --server-num=1 python2 {path_to_rdpy}/rdpscreenshot.py"
    count_cor = args.senders
    # region limits input Queue
    queue_limit_targets = count_cor
    sleep_duration_queue_full = args.queue_sleep
    # endregion
    count_input = 0
    count_good = 0
    count_error = 0
    start_time = datetime.datetime.now()

    loop = asyncio.get_event_loop()
    queue_input = asyncio.Queue()
    queue_results = asyncio.Queue()
    queue_prints = asyncio.Queue()
    read_input = method_create_targets(queue_input, settings, path_to_file_targets)  # create targets
    create_tasks = work_with_create_tasks_queue(queue_input, queue_results, queue_prints, count_cor)  # execution
    execute_tasks = work_with_queue_tasks(queue_results, queue_prints)
    print_output = work_with_queue_result(queue_prints, output_file, mode_write)
    loop.run_until_complete(
        asyncio.gather(
            read_input,
            create_tasks,
            execute_tasks,
            print_output))
    loop.close()
