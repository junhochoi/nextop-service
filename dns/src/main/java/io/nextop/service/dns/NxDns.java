package io.nextop.service.dns;


public class NxDns {
    private final int port;



    public void run() {

    }



    public static void main(String[] in) {
        try {
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.option(ChannelOption.SO_BACKLOG, 1024);
                b.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new HttpServerInitializer());

                Channel ch = b.bind(1800).sync().channel();
                ch.closeFuture().sync();
            } finally {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        } catch (InterruptedException e) {

        }
    }

    private static final class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();

            // Uncomment the following line if you want HTTPS
            //SSLEngine engine = SecureChatSslContextFactory.getServerContext().createSSLEngine();
            //engine.setUseClientMode(false);
            //p.addLast("ssl", new SslHandler(engine));

            p.addLast("codec", new HttpServerCodec());
            p.addLast("handler", new HttpServerHandler());
        }
    }


    private static final  class HttpServerHandler extends ChannelHandlerAdapter {
        private static final byte[] CONTENT = { 'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd' };

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        // FIXME
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpRequest) {
                HttpRequest req = (HttpRequest) msg;

                if (is100ContinueExpected(req)) {
                    ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
                }
                boolean keepAlive = isKeepAlive(req);
                FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(CONTENT));
                response.headers().set(CONTENT_TYPE, "text/plain");
                response.headers().set(CONTENT_LENGTH, response.content().readableBytes());

                if (!keepAlive) {
                    ctx.write(response).addListener(ChannelFutureListener.CLOSE);
                } else {
                    response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
                    ctx.write(response);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }


}
