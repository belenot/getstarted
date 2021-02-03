package com.belenot.getstarted.netty

import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import kotlin.concurrent.thread

val serverLogger = LoggerFactory.getLogger("server")
val clientLogger = LoggerFactory.getLogger("client")

fun main(args: Array<String>) {
    val port = args.getOrElse(0) { "8080" }.toInt()
    thread {
        DiscardServer(port)
            .also { serverLogger.info("Started server on ${port} port.") }
            .run()
    }
    Thread.sleep(500)

    val host = "localhost"
    val workGroup = NioEventLoopGroup()
    try {
        val bootstrap = Bootstrap()
        bootstrap.group(workGroup)
        bootstrap.channel(NioSocketChannel::class.java)
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true)
        bootstrap.handler(object : ChannelInitializer<SocketChannel>() {
            override fun initChannel(ch: SocketChannel?) {
                ch?.pipeline()?.addLast(TimeDecoder(), TimeClientHandler())
            }
        })
        val channelFuture = bootstrap.connect(host, port).sync()
        channelFuture.channel().closeFuture().sync()

    } finally {
        workGroup.shutdownGracefully()
    }
}

data class UnixTime(val value: Long) {
    constructor(): this(System.currentTimeMillis() / 1000L + 2208988800L)
}

class TimeDecoder: ByteToMessageDecoder() {
    override fun decode(ctx: ChannelHandlerContext?, `in`: ByteBuf?, out: MutableList<Any>?) {
        if (`in`?.readableBytes() != null && `in`.readableBytes() >= 4) {
            out?.add(UnixTime(`in`.readUnsignedInt()))
        }
    }
}

class TimeEncoder: ChannelOutboundHandlerAdapter() {
    override fun write(ctx: ChannelHandlerContext?, msg: Any?, promise: ChannelPromise?) {
        if (msg is UnixTime) {
            val byteBuf = ctx?.alloc()?.buffer(4)
            byteBuf?.writeInt(msg.value.toInt())
            ctx?.write(byteBuf, promise)
        }
    }
}

class TimeClientHandler: ChannelInboundHandlerAdapter() {

    override fun channelRead(ctx: ChannelHandlerContext?, msg: Any?) {
        try {
            if (msg is UnixTime) {
                clientLogger.info("Current time is ${LocalDateTime.ofInstant(Instant.ofEpochMilli((msg.value - 2208988800L) * 1000L), ZoneId.systemDefault())}")
            }
        } finally {
            ctx?.close()
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext?, cause: Throwable?) {
        cause?.printStackTrace()
        ctx?.close()
    }
}

class DiscardServerHandler : ChannelInboundHandlerAdapter() {
    override fun channelRead(ctx: ChannelHandlerContext?, msg: Any?) {
        serverLogger.info("Recieved connections.")
        if (msg is ByteBuf) {
            try {
                ctx?.writeAndFlush(msg)
                serverLogger.info("Echo reply.")
            } finally {
                serverLogger.info("Release buffer.")
//                msg.release()
            }
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext?, cause: Throwable?) {
        cause?.printStackTrace()
        ctx?.close()
    }
}

class TimeServerHandler: ChannelInboundHandlerAdapter() {
    override fun channelActive(ctx: ChannelHandlerContext?) {
        if (ctx != null) {
            serverLogger.info("Request.")
            val unixTime = UnixTime(System.currentTimeMillis() / 1000L + 2208988800L)
            val channelFuture = ctx.writeAndFlush(unixTime)
            channelFuture.addListener(ChannelFutureListener.CLOSE)
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext?, cause: Throwable?) {
        if (ctx != null && cause != null) {
            cause.printStackTrace()
            ctx.close()
        }
    }
}

class DiscardServer(private val port: Int) {
    fun run() {
        val bossGroup: EventLoopGroup = NioEventLoopGroup()
        val workerGroup: EventLoopGroup = NioEventLoopGroup()

        try {
            val serverBootstrap = ServerBootstrap()
            serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel::class.java)
                .childHandler(object : ChannelInitializer<SocketChannel>() {
                    override fun initChannel(ch: SocketChannel?) {
                        //ch?.pipeline()?.addLast(DiscardServerHandler())
                        ch?.pipeline()?.addLast(TimeEncoder(), TimeServerHandler())
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
            val channelFuture = serverBootstrap.bind(port).sync()
            channelFuture.channel().closeFuture().sync()
        } finally {
            workerGroup.shutdownGracefully()
            bossGroup.shutdownGracefully()
        }
    }
}