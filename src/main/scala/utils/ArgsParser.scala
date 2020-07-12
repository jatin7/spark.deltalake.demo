package utils

import net.bmjames.opts._
import scalaz.Apply

case class Opts(input: Option[String], output: Option[String],
                kafkaBootstrap: Option[String], group: Option[String],
                checkpoint: Option[String],rate: Option[String]) {
  override def toString: String = {
    s"input=$input, output=$output,  bootstrapServers=$kafkaBootstrap"
  }
}

object ArgsParser {
  val parseOpts: Parser[Opts] =
    ^^^^(
      optional(strOption(short('i'), long("input"))),
      optional(strOption(short('o'), long("output"))),
      optional(strOption(short('k'), long("kafka.bootstrap.servers"))),
      optional(strOption(short('g'), long("group"))),
      optional(strOption(short('c'), long("checkpoint"))),
      optional(strOption(short('r'), long("rate")))
    )(Opts.apply)

  def ^^^^[F[_], A, B, C, D, E,G,  L](fa: => F[A], fb: => F[B], fc: => F[C], fd: => F[D],
                                               fe: => F[E],fg: => F[G])(
                                                f: (A, B, C, D, E,G) => L)(implicit F: Apply[F]): F[L] =
    F.apply6(fa, fb, fc, fd, fe,fg)(f)
}
