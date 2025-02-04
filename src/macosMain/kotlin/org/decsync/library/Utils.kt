/**
 * libdecsync - Utils.kt
 *
 * Copyright (C) 2019 Aldo Gunsing
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

package org.decsync.library

import kotlinx.cinterop.*
import platform.posix.*

actual fun Int.off_t(): off_t = this.toLong()
actual val openFlagsBinary = 0
actual fun mkdirCustom(path: String, mode: Int) {
    mkdir(path, mode.toUShort())
}
actual fun readCustom(fd: Int, buf: CValuesRef<*>?, len: Int) {
    read(fd, buf, len.toULong())
}
actual fun writeCustom(fd: Int, buf: CValuesRef<*>?, size: Int) {
    write(fd, buf, size.toULong())
}
actual fun gethostnameCustom(name: CValuesRef<ByteVar>, size: Int): Int = gethostname(name, size.toULong())

actual fun getDefaultDecsyncDir(): String =
        getenv("DECSYNC_DIR")?.toKString() ?: getenv("HOME")!!.toKString() + "/DecSync"